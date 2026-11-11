package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/rs/zerolog"
	"go.mau.fi/mautrix-imessage/database"
	"go.mau.fi/mautrix-imessage/imessage"
	"go.mau.fi/util/dbutil"
	"gopkg.in/yaml.v3"
	"maunium.net/go/mautrix/appservice"
	"maunium.net/go/mautrix/bridge/bridgeconfig"
	"maunium.net/go/mautrix/id"
	"maunium.net/go/mautrix/sqlstatestore"
)

const provisioningTestSecret = "synthetic-provisioning-secret"

type provisioningTestAPI struct {
	*groupTestAPI
	contacts                                              []*imessage.Contact
	contactErr                                            error
	contactCalls, searchCalls, resolveCalls, prepareCalls atomic.Int32
	searchQuery                                           string
}

func (api *provisioningTestAPI) GetContactList() ([]*imessage.Contact, error) {
	api.contactCalls.Add(1)
	return api.contacts, api.contactErr
}

func (api *provisioningTestAPI) SearchContactList(query string) ([]*imessage.Contact, error) {
	api.searchCalls.Add(1)
	api.searchQuery = query
	return api.contacts, api.contactErr
}

func (api *provisioningTestAPI) ResolveIdentifier(address string) (string, error) {
	api.resolveCalls.Add(1)
	return api.groupTestAPI.ResolveIdentifier(address)
}

func (api *provisioningTestAPI) PrepareDM(string) error {
	api.prepareCalls.Add(1)
	return nil
}

func (api *provisioningTestAPI) GetContactInfo(string) (*imessage.Contact, error) { return nil, nil }

func provisioningTestBridge(t *testing.T, ready bool) (*IMBridge, *provisioningTestAPI) {
	t.Helper()
	api := &provisioningTestAPI{groupTestAPI: &groupTestAPI{chat: func(string, string) (*imessage.ChatInfo, error) { return nil, nil }}}
	mx := groupTestHandler(api)
	br := mx.bridge
	br.WebsocketHandler = mx
	br.Config.BaseConfig = &br.Bridge.Config
	br.Config.Homeserver.Domain = "example.com"
	br.Config.Homeserver.Software = bridgeconfig.SoftwareHungry
	br.Config.AppService.Bot.Avatar = "mxc://example.com/synthetic"
	br.Config.AppService.Bot.ParsedAvatar = id.ContentURI{Homeserver: "example.com", FileID: "synthetic"}
	br.Config.IMessage.Platform = "bluebubbles"
	br.Config.Provisioning.SharedSecret = provisioningTestSecret
	if err := yaml.Unmarshal([]byte("user: '@owner:example.com'\nusername_template: 'imessage_{{.}}'\ndisplayname_template: '{{.}}'\ncommand_prefix: '!imessage'\n"), &br.Config.Bridge); err != nil {
		t.Fatal(err)
	}
	br.Log = mx.log
	log := zerolog.Nop()
	br.ZLog = &log
	br.portalsByMXID = make(map[id.RoomID]*Portal)
	br.puppets = make(map[string]*Puppet)
	as := groupTestAppservice(t, mx, func(*http.Request) (*http.Response, error) {
		t.Error("unexpected Matrix request")
		return nil, errors.New("unexpected Matrix request")
	})
	br.Bot = as.BotIntent()
	as.PrepareWebsocket()
	br.initProvisioning()
	if ready {
		br.provisioning.markReady()
	}
	return br, api
}

func provisioningRequest(method, route, body string) appservice.HTTPProxyRequest {
	var rawBody json.RawMessage
	if body != "" {
		rawBody = json.RawMessage(body)
	}
	return appservice.HTTPProxyRequest{
		Method: method, Path: provisioningPrefix + route,
		Query:   url.Values{"user_id": {"@owner:example.com"}}.Encode(),
		Headers: http.Header{"Authorization": {"Bearer " + provisioningTestSecret}},
		Body:    rawBody,
	}
}

// Every WS case enters the wrapper, which delegates supported paths to AS.WebsocketHTTPProxy.
func provisioningCall(t *testing.T, br *IMBridge, req appservice.HTTPProxyRequest, websocket bool, escaped *bool) *appservice.HTTPProxyResponse {
	t.Helper()
	if websocket {
		data, err := json.Marshal(struct {
			appservice.HTTPProxyRequest
			EscapedPath *bool `json:"escaped_path,omitempty"`
		}{req, escaped})
		if err != nil {
			t.Fatal(err)
		}
		ok, value := br.provisioning.websocketHTTPProxy(appservice.WebsocketCommand{Data: data})
		if !ok {
			t.Fatalf("proxy failure: %v", value)
		}
		return value.(*appservice.HTTPProxyResponse)
	}
	u := &url.URL{Path: req.Path, RawQuery: req.Query}
	r := httptest.NewRequest(req.Method, u.String(), bytes.NewReader(req.Body))
	r.Header = req.Headers
	w := httptest.NewRecorder()
	br.AS.Router.ServeHTTP(w, r)
	return &appservice.HTTPProxyResponse{Status: w.Code, Headers: w.Header(), Body: w.Body.Bytes()}
}

func checkProvisioningResponse(t *testing.T, resp *appservice.HTTPProxyResponse, status int) {
	t.Helper()
	if resp.Status != status || resp.Headers.Get("Content-Type") != "application/json" || resp.Headers.Get("Cache-Control") != "no-store" || !json.Valid(resp.Body) || resp.Headers.Get("Location") != "" {
		t.Fatalf("unexpected response: %#v body=%s", resp, resp.Body)
	}
	if status >= 400 {
		var body map[string]string
		if json.Unmarshal(resp.Body, &body) != nil || body["errcode"] == "" || body["error"] == "" {
			t.Fatalf("missing Matrix error: %s", resp.Body)
		}
	}
}

func TestProvisioningAuthentication(t *testing.T) {
	for _, ws := range []bool{false, true} {
		for _, tc := range []struct {
			name   string
			status int
			change func(*appservice.HTTPProxyRequest)
		}{
			{"absent", 401, func(r *appservice.HTTPProxyRequest) { r.Headers = nil }},
			{"wrong", 401, func(r *appservice.HTTPProxyRequest) { r.Headers.Set("Authorization", "Bearer wrong") }},
			{"as token", 401, func(r *appservice.HTTPProxyRequest) { r.Headers.Set("Authorization", "Bearer synthetic-test-token") }},
			{"duplicate values", 401, func(r *appservice.HTTPProxyRequest) { r.Headers.Add("Authorization", "Bearer "+provisioningTestSecret) }},
			{"mixed case duplicate", 401, func(r *appservice.HTTPProxyRequest) {
				r.Headers["aUtHoRiZaTiOn"] = []string{"Bearer " + provisioningTestSecret}
			}},
			{"empty duplicate", 401, func(r *appservice.HTTPProxyRequest) { r.Headers["authorization"] = nil }},
			{"query token", 401, func(r *appservice.HTTPProxyRequest) {
				r.Headers = nil
				r.Query += "&access_token=" + provisioningTestSecret
			}},
			{"missing owner", 403, func(r *appservice.HTTPProxyRequest) { r.Query = "" }},
			{"foreign owner", 403, func(r *appservice.HTTPProxyRequest) { r.Query = "user_id=%40other%3Aexample.com" }},
			{"duplicate owner", 403, func(r *appservice.HTTPProxyRequest) { r.Query += "&" + r.Query }},
			{"foreign login", 404, func(r *appservice.HTTPProxyRequest) { r.Query += "&login_id=other" }},
			{"empty login", 404, func(r *appservice.HTTPProxyRequest) { r.Query += "&login_id=" }},
			{"duplicate login", 404, func(r *appservice.HTTPProxyRequest) { r.Query += "&login_id=unknown&login_id=unknown" }},
			{"invalid query", 400, func(r *appservice.HTTPProxyRequest) { r.Query += "&bad=%zz" }},
		} {
			t.Run(fmt.Sprintf("ws=%t/%s", ws, tc.name), func(t *testing.T) {
				br, api := provisioningTestBridge(t, true)
				for _, route := range []string{"/capabilities", "/contacts", "/create_dm/+12025550101", "/unknown"} {
					req := provisioningRequest("GET", route, "")
					tc.change(&req)
					checkProvisioningResponse(t, provisioningCall(t, br, req, ws, nil), tc.status)
				}
				if api.contactCalls.Load()+api.searchCalls.Load()+api.resolveCalls.Load()+api.prepareCalls.Load() != 0 {
					t.Fatal("denied request reached connector")
				}
			})
		}
	}
}

func TestProvisioningAuthenticationDiagnostics(t *testing.T) {
	br, _ := provisioningTestBridge(t, true)
	var logs bytes.Buffer
	logger := zerolog.New(&logs)
	br.ZLog = &logger
	for _, route := range []string{"/capabilities", "/create_dm/private-recipient@example.test"} {
		req := provisioningRequest("GET", route, "")
		req.Headers = nil
		checkProvisioningResponse(t, provisioningCall(t, br, req, true, nil), 401)
	}
	br.Config.AppService.HSToken = "synthetic-hs-secret"
	req := provisioningRequest("GET", "/capabilities", "")
	req.Headers.Set("Authorization", "Bearer "+br.Config.AppService.HSToken)
	checkProvisioningResponse(t, provisioningCall(t, br, req, true, nil), 401)
	for _, want := range []string{`"endpoint":"capabilities"`, `"endpoint":"other"`, `"auth_header_names":0`, `"bearer_scheme":false`, `"owner_matches":true`} {
		if !strings.Contains(logs.String(), want) {
			t.Fatalf("missing safe diagnostic %s", want)
		}
	}
	for _, secret := range []string{"private-recipient", provisioningTestSecret, "@owner:example.com", br.Config.AppService.HSToken} {
		if strings.Contains(logs.String(), secret) {
			t.Fatal("private value in authentication diagnostics")
		}
	}
}

func TestProvisioningMatrixAuthentication(t *testing.T) {
	for _, tc := range []struct {
		name, body, query     string
		upstream, want, calls int
	}{
		{name: "owner", body: `{"user_id":"@owner:example.com"}`, upstream: 200, want: 200, calls: 1},
		{name: "other identity", body: `{"user_id":"@other:example.com"}`, upstream: 200, want: 401, calls: 1},
		{name: "invalid token", body: `{}`, upstream: 401, want: 401, calls: 1},
		{name: "server error", body: `{"user_id":"@owner:example.com"}`, upstream: 503, want: 401, calls: 1},
		{name: "redirect", upstream: 302, want: 401, calls: 1},
		{name: "invalid json", body: `{"user_id":"@owner:example.com"} {}`, upstream: 200, want: 401, calls: 1},
		{name: "oversized", body: strings.Repeat(" ", 65537) + `{"user_id":"@owner:example.com"}`, upstream: 200, want: 401, calls: 1},
		{name: "foreign query owner", query: "user_id=other", want: 401},
		{name: "duplicate owner", query: "user_id=%40owner%3Aexample.com&user_id=%40owner%3Aexample.com", want: 401},
	} {
		t.Run(tc.name, func(t *testing.T) {
			br, api := provisioningTestBridge(t, true)
			var calls atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				calls.Add(1)
				if r.URL.Path != "/_hungryserv/owner/_matrix/client/v3/account/whoami" || r.URL.RawQuery != "" || r.Method != "GET" || r.Header.Get("Authorization") != "Bearer synthetic-matrix-token" {
					t.Error("unexpected identity validation request")
				}
				w.Header().Set("Location", "/must-not-follow")
				w.WriteHeader(tc.upstream)
				_, _ = io.WriteString(w, tc.body)
			}))
			defer server.Close()
			br.Config.Homeserver.Address = server.URL + "/_hungryserv/owner"
			req := provisioningRequest("GET", "/contacts", "")
			req.Headers.Set("Authorization", "Bearer synthetic-matrix-token")
			if tc.query != "" {
				req.Query = tc.query
			}
			checkProvisioningResponse(t, provisioningCall(t, br, req, true, nil), tc.want)
			if int(calls.Load()) != tc.calls || (tc.want != 200 && api.contactCalls.Load() != 0) {
				t.Fatal("identity or contact access boundary violated")
			}
		})
	}
}

func TestProvisioningDisabled(t *testing.T) {
	for _, tc := range []struct{ platform, secret string }{{"bluebubbles", ""}, {"bluebubbles", "disable"}, {"bluebubbles", "short"}, {"ios", provisioningTestSecret}} {
		br, _ := provisioningTestBridge(t, false)
		br.AS = appservice.Create()
		br.provisioning = nil
		br.Config.IMessage.Platform, br.Config.Provisioning.SharedSecret = tc.platform, tc.secret
		br.initProvisioning()
		if br.provisioning != nil {
			t.Fatal("disabled adapter was registered")
		}
		w := httptest.NewRecorder()
		br.AS.Router.ServeHTTP(w, httptest.NewRequest("GET", provisioningPrefix+"/capabilities", nil))
		if w.Code != http.StatusNotFound {
			t.Fatal("disabled endpoint is available")
		}
	}
}

func TestProvisioningWhoamiAvatar(t *testing.T) {
	for _, avatar := range []string{"mxc://example.com/synthetic", "", "remove"} {
		t.Run(avatar, func(t *testing.T) {
			br, _ := provisioningTestBridge(t, true)
			if err := yaml.Unmarshal([]byte(fmt.Sprintf("avatar: %q\n", avatar)), &br.Config.AppService.Bot); err != nil {
				t.Fatal(err)
			}
			resp := provisioningCall(t, br, provisioningRequest("GET", "/whoami", ""), true, nil)
			checkProvisioningResponse(t, resp, 200)
			var body struct {
				Network struct {
					Icon string `json:"network_icon"`
				} `json:"network"`
			}
			if err := json.Unmarshal(resp.Body, &body); err != nil {
				t.Fatal(err)
			}
			want := avatar
			if avatar == "remove" {
				want = ""
			}
			if body.Network.Icon != want {
				t.Fatalf("network_icon = %q, want %q", body.Network.Icon, want)
			}
		})
	}
}

func TestProvisioningMetadataAndReadiness(t *testing.T) {
	br, api := provisioningTestBridge(t, false)
	for _, ws := range []bool{false, true} {
		for route, want := range map[string]string{
			"/capabilities": `{"resolve_identifier":{"create_dm":true,"lookup_phone":true,"lookup_email":true,"lookup_username":false,"any_phone":false,"contact_list":true,"search":true},"group_creation":{}}`,
			"/logins":       `{"login_ids":["unknown"]}`, "/login/flows": `{"flows":[]}`,
		} {
			req := provisioningRequest("GET", route, "")
			req.Query += "&login_id=unknown"
			resp := provisioningCall(t, br, req, ws, nil)
			checkProvisioningResponse(t, resp, 200)
			var gotJSON, wantJSON any
			_ = json.Unmarshal(resp.Body, &gotJSON)
			_ = json.Unmarshal([]byte(want), &wantJSON)
			if !reflect.DeepEqual(gotJSON, wantJSON) {
				t.Fatalf("schema mismatch: %s", resp.Body)
			}
		}
		for _, route := range []string{"/contacts", "/search_users", "/resolve_identifier/+12025550101", "/create_dm/+12025550101"} {
			method := "GET"
			if strings.Contains(route, "search_users") || strings.Contains(route, "create_dm") {
				method = "POST"
			}
			checkProvisioningResponse(t, provisioningCall(t, br, provisioningRequest(method, route, `{"query":"Synthetic"}`), ws, nil), 503)
		}
	}
	if api.contactCalls.Load()+api.searchCalls.Load()+api.resolveCalls.Load()+api.prepareCalls.Load() != 0 {
		t.Fatal("early request reached connector")
	}
	for _, ready := range []bool{false, true} {
		if ready {
			br.provisioning.markReady()
		}
		resp := provisioningCall(t, br, provisioningRequest("GET", "/whoami", ""), true, nil)
		checkProvisioningResponse(t, resp, 200)
		state := "CONNECTING"
		if ready {
			state = "CONNECTED"
		}
		want := map[string]any{
			"network":    map[string]any{"displayname": "iMessage", "network_url": "https://www.apple.com/ios/messages/", "network_icon": "mxc://example.com/synthetic", "network_id": "imessage", "beeper_bridge_type": "imessage"},
			"homeserver": "example.com", "bridge_bot": string(br.Bot.UserID), "command_prefix": "!imessage", "login_flows": []any{},
			"logins": []any{map[string]any{"id": "unknown", "name": "iMessage", "profile": map[string]any{}, "state_event": state, "state_ts": float64(br.provisioning.state.Load().Timestamp), "state": map[string]any{"state_event": state, "timestamp": float64(br.provisioning.state.Load().Timestamp), "ttl": float64(3600)}}},
		}
		var got map[string]any
		_ = json.Unmarshal(resp.Body, &got)
		if !reflect.DeepEqual(got, want) || br.provisioning.state.Load().Timestamp < time.Now().Unix()-5 {
			t.Fatalf("whoami mismatch: %s", resp.Body)
		}
	}
	// OnlyBackfill avoids connector teardown in this fixture; Stop must still close the readiness gate.
	br.Config.Bridge.Backfill.OnlyBackfill = true
	br.Stop()
	br.provisioning.markReady()
	checkProvisioningResponse(t, provisioningCall(t, br, provisioningRequest("GET", "/contacts", ""), true, nil), 503)
	if br.provisioning.state.Load().Ready {
		t.Fatal("startup reopened stopping adapter")
	}
}

func TestProvisioningContactsAndSearch(t *testing.T) {
	for _, ws := range []bool{false, true} {
		br, api := provisioningTestBridge(t, true)
		api.contacts = []*imessage.Contact{
			nil,
			{Phones: []string{"+1 (202) 555-0101", "2025550102", "+01234567"}},
			{FirstName: "Synthetic", Phones: []string{"+12025550101", "+12025550102"}, Emails: []string{"ONE@EXAMPLE.COM", "unsafe+tag@example.com", "bad%name@example.com"}},
			{FirstName: "Later", Emails: []string{"one@example.com"}},
		}
		for _, route := range []string{"/contacts", "/search_users"} {
			method, key := "GET", "contacts"
			if route == "/search_users" {
				method, key = "POST", "results"
			}
			resp := provisioningCall(t, br, provisioningRequest(method, route, `{"query":"  sYnThetic  "}`), ws, nil)
			checkProvisioningResponse(t, resp, 200)
			var body map[string][]provisioningResult
			if err := json.Unmarshal(resp.Body, &body); err != nil {
				t.Fatal(err)
			}
			results := body[key]
			if len(results) != 3 {
				t.Fatalf("invalid results: %s", resp.Body)
			}
			for i, address := range []string{"+12025550101", "+12025550102", "one@example.com"} {
				want := br.provisioning.result(address)
				want.Name = "Synthetic"
				if !reflect.DeepEqual(results[i], want) {
					t.Fatalf("wrong identity/metadata: %#v", results[i])
				}
			}
		}
		if api.searchQuery != "sYnThetic" || api.searchCalls.Load() != 1 || api.contactCalls.Load() != 1 || api.resolveCalls.Load() != 0 || len(br.puppets) != 0 || len(br.portalsByGUID) != 0 {
			t.Fatal("listing did not preserve pure cached-contact behavior")
		}
		api.contacts = nil
		resp := provisioningCall(t, br, provisioningRequest("GET", "/contacts", ""), ws, nil)
		if string(bytes.TrimSpace(resp.Body)) != `{"contacts":[]}` {
			t.Fatalf("empty list is not an array: %s", resp.Body)
		}
		for i := 0; i < 1100; i++ {
			api.contacts = append(api.contacts, &imessage.Contact{Phones: []string{fmt.Sprintf("+1202555%04d", i)}})
		}
		resp = provisioningCall(t, br, provisioningRequest("GET", "/contacts", ""), ws, nil)
		var body struct {
			Contacts []provisioningResult `json:"contacts"`
		}
		_ = json.Unmarshal(resp.Body, &body)
		if len(body.Contacts) != 1100 {
			t.Fatal("silently truncated contacts")
		}
	}
}

func TestProvisioningValidationAndSafeDiagnostics(t *testing.T) {
	br, api := provisioningTestBridge(t, true)
	var logs bytes.Buffer
	logger := zerolog.New(&logs)
	br.ZLog = &logger
	for _, body := range []string{"", "null", "[]", "{", `{"query":""}`, `{"query":"  "}`, `{"query":12}`, `{"query":"valid"} {}`, `{"query":"` + strings.Repeat("a", 257) + `"}`, `{"query":"valid","padding":"` + strings.Repeat("a", 65536) + `"}`} {
		req := provisioningRequest("POST", "/search_users", "")
		req.Body = json.RawMessage(body)
		checkProvisioningResponse(t, provisioningCall(t, br, req, false, nil), 400)
	}
	for _, ws := range []bool{false, true} {
		for _, method := range []string{"POST", "OPTIONS", "HEAD"} {
			checkProvisioningResponse(t, provisioningCall(t, br, provisioningRequest(method, "/contacts", ""), ws, nil), 405)
		}
		checkProvisioningResponse(t, provisioningCall(t, br, provisioningRequest("GET", "/missing", ""), ws, nil), 404)
		for _, raw := range []string{"unsafe+tag@example.com", "2025550101", "iMessage;+;group", "SMS;-;+12025550101", "@imessage_12025550101:foreign.example", "@imessage_012025550101:example.com", "@imessage_ONE=40example.com:example.com", "+12025550101/extra", "%2B12025550101"} {
			checkProvisioningResponse(t, provisioningCall(t, br, provisioningRequest("GET", "/resolve_identifier/"+raw, ""), ws, nil), 400)
		}
	}
	if api.searchCalls.Load()+api.resolveCalls.Load()+api.contactCalls.Load() != 0 {
		t.Fatal("invalid input reached connector")
	}
	api.contactErr = errors.New("sensitive-contact-response unsafe+tag@example.com " + provisioningTestSecret)
	for _, ws := range []bool{false, true} {
		resp := provisioningCall(t, br, provisioningRequest("GET", "/contacts", ""), ws, nil)
		checkProvisioningResponse(t, resp, 502)
		if strings.Contains(string(resp.Body), "sensitive") {
			t.Fatal("upstream error leaked")
		}
	}
	for _, sensitive := range []string{"unsafe", "example.com", "sensitive", provisioningTestSecret, "query", "Authorization"} {
		if strings.Contains(logs.String(), sensitive) {
			t.Fatalf("adapter diagnostics leaked %s", sensitive)
		}
	}
	if !strings.Contains(logs.String(), `"operation":"contacts","status":502`) {
		t.Fatal("missing categorical diagnostics")
	}
}

func TestProvisioningRecipientTransportsAndMergedRoom(t *testing.T) {
	br, api := provisioningTestBridge(t, true)
	const phone, email = "+12025550101", "one@example.com"
	portal := addGroupTestPortal(t, br.WebsocketHandler, "iMessage;-;"+phone)
	portal.LastSeenHandle = "iMessage;-;" + email
	portal.Update(nil)
	br.DB.MergedChat.Set(nil, portal.GUID, "iMessage;-;"+email)
	api.contacts = []*imessage.Contact{
		{FirstName: "Wrong suffix", Phones: []string{"+2025550101"}},
		{FirstName: "Synthetic", Phones: []string{phone}, Emails: []string{email}},
	}
	var resolved []string
	api.resolve = func(address string) (string, error) {
		resolved = append(resolved, address)
		return "iMessage;-;" + address, nil
	}
	for _, ws := range []bool{false, true} {
		for _, address := range []string{phone, email} {
			for _, input := range []string{address, br.provisioning.result(address).Identifiers[0], string(br.FormatPuppetMXID(address))} {
				for _, create := range []bool{false, true} {
					method, route := "GET", "/resolve_identifier/"
					if create {
						method, route = "POST", "/create_dm/"
					}
					for _, mode := range []string{"decoded", "explicit-decoded", "escaped"} {
						if !ws && mode != "decoded" {
							continue
						}
						var escaped *bool
						value := input
						if mode != "decoded" {
							flag := mode == "escaped"
							escaped = &flag
							if flag {
								value = strings.ReplaceAll(url.PathEscape(input), "+", "%2B")
							}
						}
						before := len(resolved)
						resp := provisioningCall(t, br, provisioningRequest(method, route+value, ""), ws, escaped)
						checkProvisioningResponse(t, resp, 200)
						var result provisioningResult
						_ = json.Unmarshal(resp.Body, &result)
						if result.ID != address || result.MXID != br.FormatPuppetMXID(address) || result.DMRoomMXID != portal.roomID() || result.Name != "Synthetic" || len(resolved) != before+1 || resolved[before] != address {
							t.Fatalf("recipient round trip changed: %s", resp.Body)
						}
					}
				}
			}
		}
	}
	if portal.LastSeenHandle != "iMessage;-;"+email || api.prepareCalls.Load() != 0 {
		t.Fatal("selection changed merged routing or recreated an existing room")
	}
	api.contactErr = errors.New("synthetic refresh failure")
	resp := provisioningCall(t, br, provisioningRequest("GET", "/resolve_identifier/"+phone, ""), true, nil)
	checkProvisioningResponse(t, resp, 200)
	var result provisioningResult
	_ = json.Unmarshal(resp.Body, &result)
	if result.Name != "" {
		t.Fatal("refresh failure did not omit name")
	}
	api.resolve = func(string) (string, error) { return "", errors.New("sensitive remote response " + phone) }
	resp = provisioningCall(t, br, provisioningRequest("POST", "/create_dm/"+phone, ""), true, nil)
	checkProvisioningResponse(t, resp, 502)
	if strings.Contains(string(resp.Body), phone) || strings.Contains(string(resp.Body), "sensitive") {
		t.Fatal("resolver error leaked contact data")
	}
}

func TestProvisioningPathContracts(t *testing.T) {
	br, api := provisioningTestBridge(t, true)
	checkProvisioningResponse(t, provisioningCall(t, br, provisioningRequest("GET\n", "/contacts", ""), true, nil), 400)
	for _, flag := range []*bool{nil, new(bool)} {
		for _, input := range []string{"%2B12025550101", "one%40example.com", "%40imessage_12025550101%3Aexample.com", "%252B12025550101", "%zz"} {
			checkProvisioningResponse(t, provisioningCall(t, br, provisioningRequest("GET", "/resolve_identifier/"+input, ""), true, flag), 400)
		}
	}
	flag := true
	for _, input := range []string{"%252B12025550101", "%zz", "%2F..%2Fcontacts", "%00"} {
		checkProvisioningResponse(t, provisioningCall(t, br, provisioningRequest("GET", "/resolve_identifier/"+input, ""), true, &flag), 400)
	}
	for _, route := range []string{"//contacts", "/./contacts", "/resolve_identifier/../contacts", "/contacts/"} {
		req := provisioningRequest("GET", route, "")
		checkProvisioningResponse(t, provisioningCall(t, br, req, true, nil), 400)
		// The parent mux cleans paths before dispatch. The adapter itself never redirects.
		w := httptest.NewRecorder()
		r := httptest.NewRequest("GET", req.Path+"?"+req.Query, nil)
		r.Header = req.Headers
		br.provisioning.ServeHTTP(w, r)
		checkProvisioningResponse(t, &appservice.HTTPProxyResponse{Status: w.Code, Headers: w.Header(), Body: w.Body.Bytes()}, 400)
	}
	for _, rawFlag := range []string{`"true"`, `null`, `1`} {
		req := provisioningRequest("GET", "/contacts", "")
		data, _ := json.Marshal(req)
		data = append(data[:len(data)-1], []byte(`,"escaped_path":`+rawFlag+`}`)...)
		ok, value := br.provisioning.websocketHTTPProxy(appservice.WebsocketCommand{Data: data})
		if !ok {
			t.Fatal("proxy error was not JSON")
		}
		checkProvisioningResponse(t, value.(*appservice.HTTPProxyResponse), 400)
	}
	if api.contactCalls.Load()+api.resolveCalls.Load() != 0 {
		t.Fatal("invalid path reached connector")
	}
	// Direct HTTP decodes once; double encoding must not be guessed.
	for _, encoded := range []string{"%2B12025550101", "one%40example.com"} {
		address, _ := url.PathUnescape(encoded)
		br.portalsByGUID["iMessage;-;"+address] = &Portal{Portal: &database.Portal{MXID: "!existing:example.com"}}
		w := httptest.NewRecorder()
		r := httptest.NewRequest("GET", provisioningPrefix+"/resolve_identifier/"+encoded+"?user_id=%40owner%3Aexample.com", nil)
		r.Header.Set("Authorization", "Bearer "+provisioningTestSecret)
		br.AS.Router.ServeHTTP(w, r)
		checkProvisioningResponse(t, &appservice.HTTPProxyResponse{Status: w.Code, Headers: w.Header(), Body: w.Body.Bytes()}, 200)
	}
	// An unrelated proxy route retains the old decoded-path behavior.
	br.AS.Router.HandleFunc("/synthetic/%2B", func(w http.ResponseWriter, r *http.Request) {
		provisioningJSON(w, 200, map[string]string{"path": r.URL.Path})
	})
	req := provisioningRequest("GET", "", "")
	req.Path = "/synthetic/%2B"
	resp := provisioningCall(t, br, req, true, &flag)
	checkProvisioningResponse(t, resp, 200)
	if !strings.Contains(string(resp.Body), `/synthetic/%2B`) {
		t.Fatal("wrapper changed unrelated proxy paths")
	}
}

func TestProvisioningConcurrentCreateDM(t *testing.T) {
	br, api := provisioningTestBridge(t, true)
	const address = "+12025550101"
	portal := addGroupTestPortal(t, br.WebsocketHandler, "iMessage;-;"+address)
	portal.MXID = ""
	portal.Update(nil)
	br.user = &User{User: &database.User{MXID: br.Config.Bridge.User}, bridge: br}
	br.StateStore = sqlstatestore.NewSQLStateStore(br.DB.Database, dbutil.NoopLogger, false)
	if err := br.StateStore.Upgrade(); err != nil {
		t.Fatal(err)
	}
	puppet := br.GetPuppetByLocalID(address)
	puppet.Displayname, puppet.ContactInfoSet = address, true
	puppet.Update()
	var creates, sends atomic.Int32
	br.AS.HTTPClient.Transport = matrixTestTransport(func(req *http.Request) (*http.Response, error) {
		body := `{}`
		if strings.HasSuffix(req.URL.Path, "/register") {
			body = `{"user_id":"` + string(puppet.MXID) + `"}`
		} else if strings.HasSuffix(req.URL.Path, "/createRoom") {
			creates.Add(1)
			body = `{"room_id":"!created:example.com"}`
		} else if strings.Contains(req.URL.Path, "/send/") {
			sends.Add(1)
		}
		return &http.Response{StatusCode: 200, Body: io.NopCloser(strings.NewReader(body)), Header: make(http.Header)}, nil
	})
	// A pure lookup may reuse an empty DB portal, but must not create a Matrix room.
	resp := provisioningCall(t, br, provisioningRequest("GET", "/resolve_identifier/"+address, ""), true, nil)
	checkProvisioningResponse(t, resp, 200)
	if creates.Load() != 0 || api.prepareCalls.Load() != 0 {
		t.Fatal("lookup created a room")
	}
	var wg sync.WaitGroup
	responses := make(chan *appservice.HTTPProxyResponse, 12)
	for i := 0; i < 12; i++ {
		identifier := address
		if i%2 == 0 {
			identifier = string(br.FormatPuppetMXID(address))
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			responses <- provisioningCall(t, br, provisioningRequest("POST", "/create_dm/"+identifier, ""), true, nil)
		}()
	}
	wg.Wait()
	close(responses)
	var created bool
	for resp := range responses {
		if resp.Status != 200 && resp.Status != 201 {
			t.Fatalf("create failed: %s", resp.Body)
		}
		checkProvisioningResponse(t, resp, resp.Status)
		created = created || resp.Status == 201
		var result provisioningResult
		_ = json.Unmarshal(resp.Body, &result)
		if result.ID != address || result.DMRoomMXID != "!created:example.com" {
			t.Fatalf("concurrent create returned wrong identity: %s", resp.Body)
		}
	}
	if !created || creates.Load() != 1 || sends.Load() != 0 || portal.roomID() != "!created:example.com" {
		t.Fatalf("creates=%d sends=%d", creates.Load(), sends.Load())
	}
	checkProvisioningResponse(t, provisioningCall(t, br, provisioningRequest("POST", "/create_dm/"+address, ""), false, nil), 200)
}
