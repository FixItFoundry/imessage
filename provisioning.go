package main

import (
	"crypto/subtle"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/url"
	"path"
	"strings"
	"sync/atomic"
	"time"

	"go.mau.fi/mautrix-imessage/imessage"
	"go.mau.fi/mautrix-imessage/imessage/bluebubbles"
	"maunium.net/go/mautrix/appservice"
	"maunium.net/go/mautrix/id"
)

const provisioningPrefix = "/_matrix/provision/v3"

type provisioningState struct {
	Ready, Stopping bool
	Timestamp       int64
}

type provisioningAPI struct {
	bridge *IMBridge
	state  atomic.Pointer[provisioningState]
}

func (br *IMBridge) initProvisioning() {
	secret := br.Config.Provisioning.SharedSecret
	if br.Config.IMessage.Platform != "bluebubbles" || len(secret) < 16 || secret == "disable" {
		return
	}
	p := &provisioningAPI{bridge: br}
	p.state.Store(&provisioningState{Timestamp: time.Now().Unix()})
	br.provisioning = p
	// shortcut: the parent mux still cleans malformed direct HTTP paths; revisit before exposing direct provisioning HTTP.
	br.AS.Router.PathPrefix(provisioningPrefix).Handler(p)
	br.AS.SetWebsocketCommandHandler(appservice.WebsocketCommandHTTPProxy, p.websocketHTTPProxy)
}

func (p *provisioningAPI) markReady() {
	old := p.state.Load()
	if !old.Stopping && !old.Ready {
		p.state.CompareAndSwap(old, &provisioningState{Ready: true, Timestamp: time.Now().Unix()})
	}
}

func provisioningJSON(w http.ResponseWriter, status int, body any) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(body)
}

func provisioningError(code, message string) map[string]string {
	return map[string]string{"errcode": code, "error": message}
}

func canonicalProvisioningPath(value string) bool {
	return strings.HasPrefix(value, provisioningPrefix+"/") && path.Clean(value) == value && !strings.ContainsAny(value, "%\\?#\r\n\x00")
}

// The pinned proxy expects decoded paths. Only explicit escaped_path opts into one path decode.
func (p *provisioningAPI) websocketHTTPProxy(cmd appservice.WebsocketCommand) (bool, interface{}) {
	var req struct {
		appservice.HTTPProxyRequest
		EscapedPath json.RawMessage `json:"escaped_path"`
	}
	if json.Unmarshal(cmd.Data, &req) != nil {
		return true, &appservice.HTTPProxyResponse{
			Status:  http.StatusBadRequest,
			Headers: http.Header{"Content-Type": {"application/json"}, "Cache-Control": {"no-store"}},
			Body:    json.RawMessage(`{"errcode":"M_BAD_JSON","error":"Invalid proxy request"}`),
		}
	}
	decoded, decodeErr := url.PathUnescape(req.Path)
	if !strings.HasPrefix(req.Path, "/_matrix/provision") && !strings.HasPrefix(path.Clean(decoded), "/_matrix/provision") {
		return p.bridge.AS.WebsocketHTTPProxy(cmd)
	}
	var escaped bool
	invalidFlag := len(req.EscapedPath) > 0 && (string(req.EscapedPath) == "null" || json.Unmarshal(req.EscapedPath, &escaped) != nil)
	if escaped {
		req.Path = decoded
	}
	_, methodErr := http.NewRequest(req.Method, "http://localhost/", nil)
	if invalidFlag || decodeErr != nil || methodErr != nil || !canonicalProvisioningPath(req.Path) {
		return true, &appservice.HTTPProxyResponse{
			Status:  http.StatusBadRequest,
			Headers: http.Header{"Content-Type": {"application/json"}, "Cache-Control": {"no-store"}},
			Body:    json.RawMessage(`{"errcode":"M_INVALID_PARAM","error":"Invalid provisioning proxy request"}`),
		}
	}
	cmd.Data, _ = json.Marshal(req.HTTPProxyRequest)
	return p.bridge.AS.WebsocketHTTPProxy(cmd)
}

// Beeper's discovery probes can use Matrix auth, as in bridgev2 provisioning.
func (p *provisioningAPI) validateMatrixToken(r *http.Request, token string) bool {
	u, err := url.Parse(p.bridge.Config.Homeserver.Address)
	if err != nil || u.Host == "" || (u.Scheme != "https" && u.Scheme != "http") || u.User != nil || u.RawQuery != "" || u.Fragment != "" {
		return false
	}
	u.Path = strings.TrimRight(u.Path, "/") + "/_matrix/client/v3/account/whoami"
	u.RawPath = ""
	req, err := http.NewRequestWithContext(r.Context(), http.MethodGet, u.String(), nil)
	if err != nil {
		return false
	}
	req.Header.Set("Authorization", "Bearer "+token)
	client := &http.Client{Timeout: 10 * time.Second, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	resp, err := client.Do(req)
	if err != nil {
		return false
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return false
	}
	body, err := io.ReadAll(io.LimitReader(resp.Body, 65537))
	var identity struct {
		UserID string `json:"user_id"`
	}
	return err == nil && len(body) <= 65536 && json.Unmarshal(body, &identity) == nil && identity.UserID == string(p.bridge.Config.Bridge.User)
}

func (p *provisioningAPI) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	operation, status := "authentication", http.StatusOK
	respond := func(code int, body any) {
		status = code
		provisioningJSON(w, code, body)
	}
	fail := func(code int, errcode, message string) { respond(code, provisioningError(errcode, message)) }
	defer func() {
		if p.bridge.ZLog != nil {
			p.bridge.ZLog.Debug().Str("operation", operation).Int("status", status).Msg("Provisioning request")
		}
	}()
	var authorization []string
	var headerNames int
	for name, values := range r.Header {
		if strings.EqualFold(name, "Authorization") {
			headerNames++
			authorization = append(authorization, values...)
		}
	}
	query, queryErr := url.ParseQuery(r.URL.RawQuery)
	validOwner := queryErr == nil && len(query["user_id"]) == 1 && query.Get("user_id") == string(p.bridge.Config.Bridge.User)
	authenticated := false
	if headerNames == 1 && len(authorization) == 1 && strings.HasPrefix(authorization[0], "Bearer ") {
		token := strings.TrimPrefix(authorization[0], "Bearer ")
		authenticated = subtle.ConstantTimeCompare([]byte(token), []byte(p.bridge.Config.Provisioning.SharedSecret)) == 1
		if !authenticated && validOwner && token != "" {
			authenticated = p.validateMatrixToken(r, token)
		}
	}
	if !authenticated {
		if p.bridge.ZLog != nil {
			endpoint := "other"
			switch r.URL.Path {
			case provisioningPrefix + "/capabilities", provisioningPrefix + "/whoami", provisioningPrefix + "/logins", provisioningPrefix + "/login/flows":
				endpoint = strings.TrimPrefix(r.URL.Path, provisioningPrefix+"/")
			}
			p.bridge.ZLog.Debug().Str("endpoint", endpoint).Int("auth_header_names", headerNames).
				Bool("owner_matches", validOwner).
				Int("auth_values", len(authorization)).Bool("bearer_scheme", len(authorization) == 1 && strings.HasPrefix(authorization[0], "Bearer ")).
				Msg("Provisioning authentication rejected")
		}
		fail(http.StatusUnauthorized, "M_UNKNOWN_TOKEN", "A single valid provisioning bearer token is required")
		return
	}
	if queryErr != nil {
		fail(http.StatusBadRequest, "M_INVALID_PARAM", "Invalid query parameters")
		return
	}
	if !validOwner {
		fail(http.StatusForbidden, "M_FORBIDDEN", "The bridge owner is required")
		return
	}
	if logins, present := query["login_id"]; present && (len(logins) != 1 || logins[0] != "unknown") {
		fail(http.StatusNotFound, "M_NOT_FOUND", "Unknown login")
		return
	}
	operation = "unknown"
	if !canonicalProvisioningPath(r.URL.Path) {
		fail(http.StatusBadRequest, "M_INVALID_PARAM", "Invalid provisioning path")
		return
	}
	route := strings.TrimPrefix(r.URL.Path, provisioningPrefix)
	method, dataRoute, identifier := http.MethodGet, false, ""
	switch route {
	case "/capabilities", "/whoami", "/logins", "/login/flows":
		operation = strings.TrimPrefix(route, "/")
	case "/contacts":
		operation, dataRoute = "contacts", true
	case "/search_users":
		operation, dataRoute, method = "search_users", true, http.MethodPost
	default:
		if strings.HasPrefix(route, "/resolve_identifier/") {
			operation, dataRoute, identifier = "resolve_identifier", true, strings.TrimPrefix(route, "/resolve_identifier/")
		} else if strings.HasPrefix(route, "/create_dm/") {
			operation, dataRoute, method, identifier = "create_dm", true, http.MethodPost, strings.TrimPrefix(route, "/create_dm/")
		} else {
			fail(http.StatusNotFound, "M_NOT_FOUND", "Unknown provisioning endpoint")
			return
		}
	}
	if r.Method != method {
		w.Header().Set("Allow", method)
		fail(http.StatusMethodNotAllowed, "M_UNRECOGNIZED", "Unsupported method")
		return
	}
	state := p.state.Load()
	if dataRoute && !state.Ready {
		fail(http.StatusServiceUnavailable, "M_UNAVAILABLE", "Connector is not ready")
		return
	}
	switch operation {
	case "capabilities":
		respond(http.StatusOK, map[string]any{
			"resolve_identifier": map[string]bool{"create_dm": true, "lookup_phone": true, "lookup_email": true, "lookup_username": false, "any_phone": false, "contact_list": true, "search": true},
			"group_creation":     struct{}{},
		})
	case "logins":
		respond(http.StatusOK, map[string]any{"login_ids": []string{"unknown"}})
	case "login/flows":
		respond(http.StatusOK, map[string]any{"flows": []any{}})
	case "whoami":
		stateEvent := "CONNECTING"
		if state.Ready {
			stateEvent = BridgeStatusConnected
		}
		respond(http.StatusOK, map[string]any{
			"network":     map[string]any{"displayname": "iMessage", "network_url": "https://www.apple.com/ios/messages/", "network_icon": p.bridge.Config.AppService.Bot.ParsedAvatar.CUString(), "network_id": "imessage", "beeper_bridge_type": "imessage"},
			"login_flows": []any{}, "homeserver": p.bridge.Config.Homeserver.Domain,
			"bridge_bot": p.bridge.Bot.UserID, "command_prefix": p.bridge.Config.Bridge.CommandPrefix,
			"logins": []any{map[string]any{
				"id": "unknown", "name": "iMessage", "profile": struct{}{}, "state_event": stateEvent, "state_ts": state.Timestamp,
				"state": map[string]any{"state_event": stateEvent, "timestamp": state.Timestamp, "ttl": 3600},
			}},
		})
	case "contacts", "search_users":
		var contacts []*imessage.Contact
		var err error
		key := "contacts"
		if operation == "search_users" {
			var body struct {
				Query string `json:"query"`
			}
			decoder := json.NewDecoder(http.MaxBytesReader(w, r.Body, 64*1024))
			if decoder.Decode(&body) != nil || decoder.Decode(new(any)) != io.EOF || len(body.Query) > 256 || strings.TrimSpace(body.Query) == "" {
				fail(http.StatusBadRequest, "M_BAD_JSON", "Expected a single JSON object with a nonempty query of at most 256 bytes")
				return
			}
			contacts, err = p.bridge.IM.SearchContactList(strings.TrimSpace(body.Query))
			key = "results"
		} else {
			contacts, err = p.bridge.IM.GetContactList()
		}
		if err != nil {
			fail(http.StatusBadGateway, "M_UNKNOWN", "Contact lookup failed")
			return
		}
		respond(http.StatusOK, map[string]any{key: p.contactResults(contacts)})
	default:
		address, err := p.normalizeIdentifier(identifier)
		if err != nil {
			fail(http.StatusBadRequest, "M_INVALID_PARAM", "Use an international phone number, supported plain email, or canonical bridge ghost")
			return
		}
		resp, err := p.bridge.WebsocketHandler.StartChat(StartDMRequest{Identifier: address, ActuallyStart: operation == "create_dm"})
		if err != nil {
			fail(http.StatusBadGateway, "M_UNKNOWN", "Could not resolve or open this recipient; check availability and connector configuration")
			return
		}
		result := p.result(imessage.ParseIdentifier(resp.GUID).LocalID)
		result.DMRoomMXID = resp.RoomID
		if contacts, err := p.bridge.IM.GetContactList(); err == nil {
			for _, contact := range p.contactResults(contacts) {
				if contact.ID == result.ID {
					result.Name = contact.Name
					break
				}
			}
		}
		code := http.StatusOK
		if resp.JustCreated {
			code = http.StatusCreated
		}
		respond(code, result)
	}
}

type provisioningResult struct {
	ID          string    `json:"id"`
	Name        string    `json:"name,omitempty"`
	Identifiers []string  `json:"identifiers"`
	MXID        id.UserID `json:"mxid"`
	DMRoomMXID  id.RoomID `json:"dm_room_mxid,omitempty"`
}

func (p *provisioningAPI) result(address string) provisioningResult {
	scheme := "tel:"
	if strings.Contains(address, "@") {
		scheme = "mailto:"
	}
	return provisioningResult{ID: address, Identifiers: []string{scheme + address}, MXID: p.bridge.FormatPuppetMXID(address)}
}

func normalizeProvisioningAddress(input string) (string, error) {
	if len(input) > 512 || strings.ContainsAny(input, ";%/\\") {
		return "", errors.New("invalid recipient")
	}
	isEmail := strings.Contains(input, "@")
	if strings.HasPrefix(input, "mailto:") && isEmail {
		input = strings.TrimPrefix(input, "mailto:")
	} else if strings.HasPrefix(input, "tel:") && !isEmail {
		input = strings.TrimPrefix(input, "tel:")
	}
	_, address, err := bluebubbles.NormalizeRecipient(input)
	return address, err
}

func (p *provisioningAPI) normalizeIdentifier(input string) (string, error) {
	if !strings.HasPrefix(input, "@") {
		return normalizeProvisioningAddress(input)
	}
	// Bound parsing to this template and domain, then require an exact canonical round trip.
	const marker = "provisioningplaceholder"
	template := string(p.bridge.FormatPuppetMXID(marker))
	prefix, suffix, found := strings.Cut(template, marker)
	if !found || strings.Contains(suffix, marker) || len(input) > 1024 || !strings.HasPrefix(input, prefix) || !strings.HasSuffix(input, suffix) || len(input) <= len(prefix)+len(suffix) {
		return "", errors.New("invalid ghost")
	}
	local := input[len(prefix) : len(input)-len(suffix)]
	if isNumber(local) {
		local = "+" + local
	} else {
		var err error
		local, err = id.DecodeUserLocalpart(local)
		if err != nil {
			return "", errors.New("invalid ghost")
		}
	}
	address, err := normalizeProvisioningAddress(local)
	if err != nil || p.bridge.FormatPuppetMXID(address) != id.UserID(input) {
		return "", errors.New("invalid ghost")
	}
	return address, nil
}

func (p *provisioningAPI) contactResults(contacts []*imessage.Contact) []provisioningResult {
	results := make([]provisioningResult, 0)
	seen, named := make(map[string]int), make(map[string]bool)
	for _, contact := range contacts {
		if contact == nil {
			continue
		}
		for _, addresses := range [][]string{contact.Phones, contact.Emails} {
			for _, raw := range addresses {
				address, err := normalizeProvisioningAddress(raw)
				if err != nil {
					continue
				}
				if index, ok := seen[address]; ok {
					if !named[address] && contact.HasName() {
						results[index].Name, named[address] = contact.Name(), true
					}
					continue
				}
				seen[address], named[address] = len(results), contact.HasName()
				result := p.result(address)
				result.Name = contact.Name()
				results = append(results, result)
			}
		}
	}
	return results
}
