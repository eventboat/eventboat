package admin

import (
	"crypto/subtle"
	"fmt"
	"net"
	"net/http"
	"strings"
)

// Security is the access control of the whole admin listener — the Admin
// REST + SSE + UI, /metrics and /mcp share one ServeMux, so one middleware
// guards all of them: an optional bearer token plus a Host allowlist
// derived from the listen address (DNS-rebinding defense; a rebinder needs
// the victim's browser to keep its Host header, which the allowlist denies).
// The zero value applies no checks; loopback binds keep that behavior.
type Security struct {
	Token  string // bearer token; "" disables token auth (loopback only)
	Listen string // configured listen address ("host:port")
}

// NewSecurity enforces the secure-by-default rule for non-loopback binds:
// the write surface can deploy pipeline YAML whose grpc plugin `command:`
// executes on the host, so an unauthenticated network listener is remote
// code execution by design — refuse to start and ask for a token. Loopback
// without a token stays allowed (the historical local-dev behavior).
func NewSecurity(token, listen string) (Security, error) {
	s := Security{Token: token, Listen: listen}
	if s.Token == "" && !loopbackHost(s.hostOf()) {
		return s, fmt.Errorf(
			"admin listener %q is not loopback and has no token: the admin surface can deploy pipelines that execute plugin commands on this host; set --admin-token, EVENTBOAT_ADMIN_TOKEN or admin.token in the runtime config, or bind a loopback address (admin.listen)", listen)
	}
	return s, nil
}

func (s Security) hostOf() string {
	if h, _, err := net.SplitHostPort(s.Listen); err == nil {
		return strings.Trim(h, "[]")
	}
	return strings.Trim(s.Listen, "[]")
}

// loopbackHost reports whether host names this machine only. "" (a wildcard
// ":port" listen) is NOT loopback — it binds every interface.
func loopbackHost(host string) bool {
	if host == "localhost" {
		return true
	}
	ip := net.ParseIP(host)
	return ip != nil && ip.IsLoopback()
}

// allowedHosts returns the Host headers this listener answers to, or nil
// when Host checking is impractical (wildcard binds: the Host header then
// carries no signal — those binds REQUIRE a token via NewSecurity, which is
// the actual gate). Loopback binds accept every spelling of the loopback
// names with the configured port; none of those names can be DNS-rebound.
func (s Security) allowedHosts() map[string]bool {
	host := s.hostOf()
	if host == "" || host == "0.0.0.0" || host == "::" || host == "*" {
		return nil
	}
	_, port, err := net.SplitHostPort(s.Listen)
	if err != nil {
		port = ""
	}
	allowed := map[string]bool{}
	add := func(h string) {
		if port != "" {
			allowed[net.JoinHostPort(h, port)] = true
		}
		if strings.Contains(h, ":") {
			allowed["["+h+"]"] = true // bare bracketed IPv6
		} else {
			allowed[h] = true // bare host (port 80/443 clients may omit it)
		}
	}
	if loopbackHost(host) {
		add("127.0.0.1")
		add("localhost")
		add("::1")
		return allowed
	}
	add(host)
	return allowed
}

// Middleware guards every request on the listener: Host allowlist first
// (403), then the bearer token (401). Browser navigations that fail the
// token check get the sign-in page as the 401 body (data stays denied;
// humans get a way in — see loginHTML). Every response carries
// X-Content-Type-Options: nosniff — bodies include attacker-influenced
// strings (pipeline diagnostics), and a sniffed MIME must never be
// interpreted as executable content.
func (s Security) Middleware(next http.Handler) http.Handler {
	hosts := s.allowedHosts()
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Set before anything is written so it covers the 401/403 error
		// bodies, the HTML shell, SSE and /metrics alike.
		w.Header().Set("X-Content-Type-Options", "nosniff")
		if hosts != nil && !hosts[r.Host] {
			http.Error(w, "host not allowed", http.StatusForbidden)
			return
		}
		if s.Token != "" && !s.authorized(r) {
			w.Header().Set("WWW-Authenticate", `Bearer realm="eventboat admin"`)
			w.WriteHeader(http.StatusUnauthorized)
			if strings.Contains(r.Header.Get("Accept"), "text/html") {
				_, _ = w.Write([]byte(loginHTML)) // the sign-in prompt, not the console
				return
			}
			_, _ = w.Write([]byte("unauthorized\n"))
			return
		}
		next.ServeHTTP(w, r)
	})
}

// authorized accepts the token via `Authorization: Bearer <token>` (what
// curl/agents and the UI's fetches use). The `?token=` query parameter is
// accepted on the SSE endpoint only (/admin/sse): EventSource cannot set
// headers. Narrowing it to that one path keeps a token leaked in a URL
// (browser history, proxies' access logs; eventboat itself does not log
// requests) from unlocking the whole write surface — deploy executes
// pipeline plugin commands on the host. Both compare in constant time.
func (s Security) authorized(r *http.Request) bool {
	if subtle.ConstantTimeCompare(bearerToken(r.Header.Get("Authorization")), []byte(s.Token)) == 1 {
		return true
	}
	if r.URL.Path != "/admin/sse" {
		return false
	}
	return subtle.ConstantTimeCompare([]byte(r.URL.Query().Get("token")), []byte(s.Token)) == 1
}

func bearerToken(auth string) []byte {
	const prefix = "Bearer "
	if len(auth) <= len(prefix) || !strings.EqualFold(auth[:len(prefix)], prefix) {
		return nil
	}
	return []byte(auth[len(prefix):])
}

// loginHTML is the token prompt served as the 401 body for browser
// navigations. It holds no data; the entered token is verified against
// /admin/status.json (fetch + Authorization header), kept in sessionStorage
// and handed to the console by a plain redirect — ?token= is deliberately
// not used outside the SSE endpoint (see authorized for the leakage caveat).
const loginHTML = `<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<title>Eventboat — sign in</title>
<style>
  body { font-family: ui-sans-serif, system-ui, sans-serif; background: #0f1420; color: #e6e9f0; display: flex; min-height: 100vh; margin: 0; align-items: center; justify-content: center; }
  .box { background: #151d30; border: 1px solid #26304a; border-radius: 8px; padding: 24px; width: 320px; }
  h1 { font-size: 16px; margin: 0 0 8px; }
  p { color: #8b96ad; font-size: 12px; }
  input { width: 100%; box-sizing: border-box; padding: 8px; border-radius: 6px; border: 1px solid #26304a; background: #0f1420; color: #e6e9f0; }
  button { margin-top: 8px; width: 100%; padding: 8px; border: 0; border-radius: 6px; background: #223154; color: #e6e9f0; cursor: pointer; }
  #err { color: #ff9aa8; font-size: 12px; min-height: 1em; }
</style>
</head>
<body>
<div class="box">
  <h1>Eventboat</h1>
  <p>This server requires an admin token.</p>
  <input id="tok" type="password" placeholder="admin token" autofocus>
  <button onclick="go()">Sign in</button>
  <div id="err"></div>
</div>
<script>
async function go() {
  const t = document.getElementById('tok').value;
  const res = await fetch('/admin/status.json', {headers: {Authorization: 'Bearer ' + t}});
  if (!res.ok) { document.getElementById('err').textContent = 'invalid token'; return; }
  sessionStorage.setItem('eb_admin_token', t);
  location.href = '/admin/';
}
document.getElementById('tok').addEventListener('keydown', e => { if (e.key === 'Enter') go(); });
</script>
</body>
</html>
`
