package history

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/url"
	"strings"
)

// Bodies are byte slices, encoded as base64 in JSON, so invalid JSON and
// incomplete responses survive a save/load unchanged unless redacted.
type Request struct {
	Method   string      `json:"method"`
	URL      string      `json:"url"`
	Headers  http.Header `json:"headers,omitempty"`
	Body     []byte      `json:"body,omitempty"`
	Redacted bool        `json:"redacted,omitempty"`
}

type Response struct {
	Status   int         `json:"status"`
	Headers  http.Header `json:"headers,omitempty"`
	Body     []byte      `json:"body,omitempty"`
	Redacted bool        `json:"redacted,omitempty"`
}

func secretKey(key string) bool {
	key = strings.ToLower(strings.NewReplacer("-", "_", ".", "_").Replace(key))
	return key == "authorization" || key == "proxy_authorization" ||
		key == "cookie" || key == "set_cookie" || key == "token" ||
		key == "credentials" || key == "credential" || key == "password" ||
		key == "client_secret" || key == "api_key" || key == "signature" ||
		strings.HasSuffix(key, "_token") || strings.HasSuffix(key, "_password") ||
		strings.HasSuffix(key, "_secret") || strings.HasSuffix(key, "_credentials") ||
		strings.HasSuffix(key, "_credential") || strings.HasSuffix(key, "_signature") ||
		strings.HasSuffix(key, "_access_key_id") || strings.HasSuffix(key, "_secret_access_key") ||
		key == "access_key_id" || key == "secret_access_key" || key == "x_api_key"
}

// RedactText removes configured secrets and sanitizes the request URL in errors.
func RedactText(text, requestURL string, secrets ...string) string {
	if requestURL != "" {
		text = strings.ReplaceAll(text, requestURL, SafeURL(requestURL))
	}
	for _, secret := range secrets {
		if secret != "" {
			text = strings.ReplaceAll(text, secret, "[REDACTED]")
		}
	}
	return text
}

// SafeURL omits userinfo and known credential query parameters.
func SafeURL(raw string) string {
	u, err := url.Parse(raw)
	if err != nil {
		return "[invalid URL]"
	}
	changed := u.User != nil || u.Fragment != ""
	u.User = nil
	u.Fragment = ""
	u.RawFragment = ""
	q := u.Query()
	for key := range q {
		if secretKey(key) {
			q.Set(key, "[REDACTED]")
			changed = true
		}
	}
	if changed {
		u.RawQuery = q.Encode()
		return u.String()
	}
	return raw
}

func capture(headers http.Header, body []byte, secrets []string) (http.Header, []byte, bool) {
	redacted := false
	h := headers.Clone()
	for key, values := range h {
		if secretKey(key) {
			h[key] = []string{"[REDACTED]"}
			redacted = true
			continue
		}
		for i, value := range values {
			if strings.EqualFold(key, "Location") {
				value = SafeURL(value)
				redacted = redacted || value != values[i]
			}
			clean := RedactText(value, "", secrets...)
			redacted = redacted || clean != value
			h[key][i] = clean
		}
	}
	// Preserve original bytes when there are no credential fields. UseNumber
	// avoids rounding 64-bit snapshot IDs if a JSON body needs redaction.
	var value any
	decoder := json.NewDecoder(bytes.NewReader(body))
	decoder.UseNumber()
	if json.Valid(body) && decoder.Decode(&value) == nil {
		clean, changed := redactJSON(value, secrets)
		if changed {
			body, _ = json.Marshal(clean)
		}
		return h, append([]byte(nil), body...), redacted || changed
	}
	clean := []byte(RedactText(string(body), "", secrets...))
	redacted = redacted || !bytes.Equal(body, clean)
	return h, clean, redacted
}

func redactJSON(value any, secrets []string) (any, bool) {
	changed := false
	switch v := value.(type) {
	case map[string]any:
		for key, child := range v {
			if secretKey(key) {
				v[key] = "[REDACTED]"
				changed = true
			} else {
				clean, modified := redactJSON(child, secrets)
				v[key] = clean
				changed = modified || changed
			}
		}
	case []any:
		for i, child := range v {
			clean, modified := redactJSON(child, secrets)
			v[i] = clean
			changed = modified || changed
		}
	case string:
		clean := RedactText(v, "", secrets...)
		return clean, clean != v
	}
	return value, changed
}

func CaptureRequest(method, rawURL string, headers http.Header, body []byte, secrets ...string) *Request {
	h, b, redacted := capture(headers, body, secrets)
	u := RedactText(SafeURL(rawURL), "", secrets...)
	return &Request{Method: method, URL: u, Headers: h, Body: b, Redacted: redacted || u != rawURL}
}

func CaptureResponse(status int, headers http.Header, body []byte, secrets ...string) *Response {
	h, b, redacted := capture(headers, body, secrets)
	return &Response{Status: status, Headers: h, Body: b, Redacted: redacted}
}
