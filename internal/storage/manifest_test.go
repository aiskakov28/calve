package storage

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestManifestFileIsImmutable(t *testing.T) {
	path := filepath.Join(t.TempDir(), "metadata", "empty.avro")
	location := (&url.URL{Scheme: "file", Path: path}).String()
	s := ManifestStore{}
	if err := s.Put(context.Background(), location, []byte("original")); err != nil {
		t.Fatal(err)
	}
	if err := s.Put(context.Background(), location, []byte("replacement")); err == nil {
		t.Fatal("overwrote immutable object")
	}
	body, _ := os.ReadFile(path)
	if string(body) != "original" {
		t.Fatal(string(body))
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := s.Put(ctx, location+"-canceled", nil); err == nil {
		t.Fatal("canceled write succeeded")
	}
	if _, err := os.Stat(path + "-canceled"); !os.IsNotExist(err) {
		t.Fatal(err)
	}
}

func TestSignedS3ManifestUpload(t *testing.T) {
	body := []byte("real manifest bytes")
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		got, _ := io.ReadAll(r.Body)
		if !bytes.Equal(got, body) || r.Method != "PUT" || r.URL.EscapedPath() != "/gateway/bucket/a%20b%2B.avro" ||
			r.Header.Get("If-None-Match") != "*" || r.Header.Get("X-Amz-Content-Sha256") != hash(body) ||
			r.Header.Get("X-Amz-Security-Token") != "session-secret" ||
			!strings.Contains(r.Header.Get("Authorization"), "Credential=test-key/") ||
			strings.Contains(r.Header.Get("Authorization"), "private-key") {
			t.Error("malformed signed upload")
		}
		w.WriteHeader(200)
	}))
	defer server.Close()
	s := ManifestStore{Endpoint: server.URL + "/gateway", Region: "local-01", AccessKey: "test-key", SecretKey: "private-key", SessionToken: "session-secret"}
	if err := s.Put(context.Background(), "s3://bucket/a%20b%2B.avro", body); err != nil {
		t.Fatal(err)
	}
}

func TestManifestErrorsDoNotEchoCredentialsOrFollowRedirects(t *testing.T) {
	for _, code := range []int{403, 307} {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Location", "http://127.0.0.1:1/")
			w.WriteHeader(code)
			io.WriteString(w, "private-key session-secret")
		}))
		s := ManifestStore{Endpoint: server.URL, Region: "local-01", AccessKey: "test", SecretKey: "private-key", SessionToken: "session-secret"}
		err := s.Put(context.Background(), "s3://bucket/manifest.avro", nil)
		server.Close()
		if err == nil || strings.Contains(err.Error(), "private-key") || strings.Contains(err.Error(), "session-secret") ||
			strings.Contains(err.Error(), "127.0.0.1:1") {
			t.Fatal(err)
		}
	}
	for _, uri := range []string{"http://example.com/file", "file://elsewhere/file", "s3://bucket/path?credential=secret"} {
		if err := (ManifestStore{}).Put(context.Background(), uri, nil); err == nil {
			t.Fatal(uri)
		}
	}
}
