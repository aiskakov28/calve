// Package storage writes the immutable manifest-list objects used by Calve's
// snapshot workload. Catalog HTTP evidence is recorded separately.
package storage

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"time"
)

// ManifestStore supports local files and path-style S3-compatible endpoints.
// Credentials are explicit configuration, never included in history or errors.
type ManifestStore struct {
	Endpoint, Region, AccessKey, SecretKey, SessionToken string
	HTTP                                                 *http.Client
}

func (s ManifestStore) Validate() error {
	if s.Endpoint == "" {
		return nil // Local files need no object-store configuration.
	}
	u, err := url.Parse(s.Endpoint)
	if err != nil || u.Host == "" || u.Scheme != "http" && u.Scheme != "https" ||
		u.User != nil || u.RawQuery != "" || u.Fragment != "" || s.Region == "" ||
		strings.ContainsAny(s.Region, "/ \t\r\n") || s.AccessKey == "" || s.SecretKey == "" ||
		strings.ContainsAny(s.AccessKey, "/\r\n") {
		return fmt.Errorf("S3 requires an HTTP(S) endpoint, region, and access/secret keys")
	}
	return nil
}

// Put creates one immutable object. A failed upload is never followed by a
// snapshot commit that references it; neither backend overwrites existing files.
func (s ManifestStore) Put(ctx context.Context, location string, body []byte) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	u, err := url.Parse(location)
	if err != nil || u.User != nil || u.RawQuery != "" || u.Fragment != "" {
		return fmt.Errorf("invalid manifest URI")
	}
	if u.Scheme == "file" && (u.Host == "" || u.Host == "localhost") {
		name := u.Path
		if runtime.GOOS == "windows" && len(name) > 3 && name[0] == '/' && name[2] == ':' {
			name = name[1:]
		}
		name = filepath.FromSlash(name)
		if !filepath.IsAbs(name) {
			return fmt.Errorf("manifest file URI must be absolute")
		}
		if err := os.MkdirAll(filepath.Dir(name), 0700); err != nil {
			return err
		}
		file, err := os.OpenFile(name, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
		if err != nil {
			return err
		}
		_, writeErr := file.Write(body)
		closeErr := file.Close()
		if writeErr != nil {
			return writeErr
		}
		return closeErr
	}
	if u.Scheme != "s3" || u.Host == "" || u.Port() != "" || u.Path == "" {
		return fmt.Errorf("manifest storage supports absolute file: and s3: URIs")
	}
	if s.Endpoint == "" {
		return fmt.Errorf("S3 manifest storage requires --s3-endpoint and AWS credentials")
	}
	if err := s.Validate(); err != nil {
		return err
	}
	endpoint, _ := url.Parse(s.Endpoint)
	endpoint.Path = strings.TrimRight(endpoint.Path, "/") + "/" + u.Host + u.Path
	endpoint.RawPath = s3Path(endpoint.Path)
	req, err := http.NewRequestWithContext(ctx, http.MethodPut, endpoint.String(), bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("create manifest upload request")
	}
	req.Header.Set("If-None-Match", "*")
	sign(req, body, s, time.Now().UTC())
	client := s.HTTP
	if client == nil {
		client = &http.Client{Timeout: 10 * time.Second, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	}
	response, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("manifest upload: %w", err)
	}
	defer response.Body.Close()
	// Do not include object-store error bodies, which can echo credentials.
	_, readErr := io.Copy(io.Discard, io.LimitReader(response.Body, 64<<10))
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		return fmt.Errorf("manifest upload: HTTP %d", response.StatusCode)
	}
	return readErr
}

func s3Path(path string) string {
	const digits = "0123456789ABCDEF"
	var out strings.Builder
	for i := 0; i < len(path); i++ {
		c := path[i]
		if c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9' || strings.ContainsRune("-._~/", rune(c)) {
			out.WriteByte(c)
		} else {
			out.WriteByte('%')
			out.WriteByte(digits[c>>4])
			out.WriteByte(digits[c&15])
		}
	}
	return out.String()
}

func hash(body []byte) string { value := sha256.Sum256(body); return hex.EncodeToString(value[:]) }
func mac(key []byte, text string) []byte {
	h := hmac.New(sha256.New, key)
	h.Write([]byte(text))
	return h.Sum(nil)
}

func sign(req *http.Request, body []byte, s ManifestStore, now time.Time) {
	date, stamp := now.Format("20060102"), now.Format("20060102T150405Z")
	payload := hash(body)
	req.Header.Set("X-Amz-Date", stamp)
	req.Header.Set("X-Amz-Content-Sha256", payload)
	headers := "host:" + req.URL.Host + "\nif-none-match:*\nx-amz-content-sha256:" + payload + "\nx-amz-date:" + stamp + "\n"
	names := "host;if-none-match;x-amz-content-sha256;x-amz-date"
	if s.SessionToken != "" {
		req.Header.Set("X-Amz-Security-Token", s.SessionToken)
		headers += "x-amz-security-token:" + strings.Join(strings.Fields(s.SessionToken), " ") + "\n"
		names += ";x-amz-security-token"
	}
	scope := date + "/" + s.Region + "/s3/aws4_request"
	canonical := req.Method + "\n" + req.URL.EscapedPath() + "\n\n" + headers + "\n" + names + "\n" + payload
	key := mac(mac(mac(mac([]byte("AWS4"+s.SecretKey), date), s.Region), "s3"), "aws4_request")
	signature := hex.EncodeToString(mac(key, "AWS4-HMAC-SHA256\n"+stamp+"\n"+scope+"\n"+hash([]byte(canonical))))
	req.Header.Set("Authorization", "AWS4-HMAC-SHA256 Credential="+s.AccessKey+"/"+scope+", SignedHeaders="+names+", Signature="+signature)
}
