package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/rs/zerolog"
	"go.mau.fi/mautrix-imessage/config"
	"go.mau.fi/mautrix-imessage/database"
	"go.mau.fi/mautrix-imessage/imessage"
	"go.mau.fi/util/dbutil"
	"maunium.net/go/maulogger/v2"
	"maunium.net/go/mautrix/appservice"
	"maunium.net/go/mautrix/bridge/bridgeconfig"
	"maunium.net/go/mautrix/id"
	"maunium.net/go/mautrix/sqlstatestore"
)

type groupTestAPI struct {
	imessage.API
	resolve func(string) (string, error)
	create  func([]string) (*imessage.CreateGroupResponse, error)
	chat    func(string, string) (*imessage.ChatInfo, error)
}

func (api *groupTestAPI) ResolveIdentifier(identifier string) (string, error) {
	if api.resolve != nil {
		return api.resolve(identifier)
	}
	if strings.Contains(identifier, ";") {
		return identifier, nil
	}
	return "iMessage;-;" + identifier, nil
}

func (api *groupTestAPI) CreateGroup(guids []string) (*imessage.CreateGroupResponse, error) {
	return api.create(guids)
}

func (api *groupTestAPI) GetChatInfo(guid, threadID string) (*imessage.ChatInfo, error) {
	return api.chat(guid, threadID)
}

func (api *groupTestAPI) GetGroupAvatar(string) (*imessage.Attachment, error) { return nil, nil }
func (api *groupTestAPI) Capabilities() imessage.ConnectorCapabilities {
	return imessage.ConnectorCapabilities{}
}

type messageGroupTestAPI struct {
	*groupTestAPI
	createMessage func([]string, string) (*imessage.CreateGroupResponse, error)
}

func (api *messageGroupTestAPI) CreateGroupWithMessage(guids []string, message string) (*imessage.CreateGroupResponse, error) {
	return api.createMessage(guids, message)
}

func groupTestHandler(api imessage.API) *WebsocketCommandHandler {
	return &WebsocketCommandHandler{
		bridge: &IMBridge{IM: api, Config: &config.Config{}, portalsByGUID: make(map[string]*Portal)},
		log:    maulogger.Create(),
	}
}

func addGroupTestPortal(t *testing.T, mx *WebsocketCommandHandler, guid string) *Portal {
	t.Helper()
	db, err := dbutil.NewWithDialect(":memory:", "sqlite3")
	if err != nil {
		t.Fatal(err)
	}
	db.RawDB.SetMaxOpenConns(1)
	t.Cleanup(func() { _ = db.Close() })
	mx.bridge.DB = database.New(db, mx.log)
	if err = mx.bridge.DB.Upgrade(); err != nil {
		t.Fatal(err)
	}
	dbPortal := mx.bridge.DB.Portal.New()
	dbPortal.GUID, dbPortal.MXID = guid, "!test:example.com"
	dbPortal.Insert(nil)
	portal := &Portal{Portal: dbPortal, Identifier: imessage.ParseIdentifier(guid), bridge: mx.bridge, log: mx.log, zlog: zerolog.Nop()}
	mx.bridge.portalsByGUID[guid] = portal
	return portal
}

func groupTestRequest() CreateGroupRequest {
	return CreateGroupRequest{
		Users: []string{"+12025550101", "+12025550102"}, Message: "  first message\n", TransactionID: "$group:example.com",
	}
}

func TestCreateGroupLegacyDispatch(t *testing.T) {
	var calls int
	api := &groupTestAPI{create: func(guids []string) (*imessage.CreateGroupResponse, error) {
		calls++
		if !reflect.DeepEqual(guids, []string{"iMessage;-;+12025550101", "iMessage;-;+12025550102"}) {
			t.Fatalf("unexpected GUIDs: %#v", guids)
		}
		return &imessage.CreateGroupResponse{GUID: "iMessage;+;legacy", ThreadID: "thread"}, nil
	}}
	mx := groupTestHandler(api)
	portal := addGroupTestPortal(t, mx, "iMessage;+;legacy")
	data := json.RawMessage(`{"users":["iMessage;-;+12025550101","+12025550102"],"allow_sms":false}`)
	ok, result := mx.handleWSCreateGroup(appservice.WebsocketCommand{Data: data})
	if !ok || result.(*StartDMResponse).GUID != "iMessage;+;legacy" || calls != 1 {
		t.Fatalf("legacy create failed: %v, %v", ok, result)
	}
	if portal.ThreadID != "thread" {
		t.Fatal("legacy creation did not retain its response thread ID")
	}
	if _, err := mx.CreateGroup(groupTestRequest()); err == nil || !strings.Contains(err.Error(), "does not support") || calls != 1 {
		t.Fatalf("legacy connector must not discard message: %v", err)
	}
}

func TestCreateGroupMessageReplayPreservesEchoMetadata(t *testing.T) {
	var calls, resolutions int
	api := &messageGroupTestAPI{groupTestAPI: &groupTestAPI{resolve: func(identifier string) (string, error) {
		resolutions++
		return identifier, nil
	}}, createMessage: func(guids []string, message string) (*imessage.CreateGroupResponse, error) {
		calls++
		if message != "  first message\n" || len(guids) != 2 {
			t.Fatal("first message changed")
		}
		return &imessage.CreateGroupResponse{GUID: "iMessage;+;new-group", ThreadID: "response-thread"}, nil
	}}
	mx := groupTestHandler(api)
	portal := addGroupTestPortal(t, mx, "iMessage;+;new-group")
	// The first-message echo may have already populated and persisted this portal.
	portal.ThreadID = "echo-thread"
	portal.Update(nil)
	req := groupTestRequest()
	req.Users = []string{"iMessage;-;+12025550101", "iMessage;-;+12025550102"}
	first, err := mx.CreateGroup(req)
	if err != nil || first.GUID != portal.GUID || first.RoomID != portal.MXID || first.JustCreated {
		t.Fatalf("unexpected response: %#v, %v", first, err)
	}
	first.GUID = "caller mutation"
	second, err := mx.CreateGroup(req)
	if err != nil || second.GUID != portal.GUID || calls != 1 || resolutions != 2 {
		t.Fatalf("replay was not retained: %#v, %v, calls=%d resolutions=%d", second, err, calls, resolutions)
	}
	if portal.ThreadID != "echo-thread" || mx.bridge.DB.Portal.GetByGUID(portal.GUID).ThreadID != "echo-thread" {
		t.Fatal("creation overwrote echo-populated thread metadata")
	}
	if ttl := time.Until(mx.groupCreates[req.TransactionID].expiresAt); ttl < 23*time.Hour || ttl > 24*time.Hour {
		t.Fatalf("incorrect transaction retention: %s", ttl)
	}
	req.Message += "changed"
	if _, err = mx.CreateGroup(req); err == nil || !strings.Contains(err.Error(), "different group request") || calls != 1 {
		t.Fatalf("transaction payload mismatch was not rejected: %v", err)
	}
}

func TestCreateGroupConcurrentResponseMutation(t *testing.T) {
	api := &messageGroupTestAPI{groupTestAPI: &groupTestAPI{}, createMessage: func([]string, string) (*imessage.CreateGroupResponse, error) {
		return &imessage.CreateGroupResponse{GUID: "iMessage;+;new-group"}, nil
	}}
	mx := groupTestHandler(api)
	addGroupTestPortal(t, mx, "iMessage;+;new-group")
	req := groupTestRequest()
	if _, err := mx.CreateGroup(req); err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			resp, err := mx.CreateGroup(req)
			if err != nil || resp.GUID != "iMessage;+;new-group" {
				t.Errorf("cached response changed: %#v, %v", resp, err)
				return
			}
			resp.GUID = "caller mutation"
		}()
	}
	wg.Wait()
}

func TestCreateGroupSMSConsentAndMatrixFailure(t *testing.T) {
	var calls, matrixCalls int
	api := &messageGroupTestAPI{groupTestAPI: &groupTestAPI{}, createMessage: func([]string, string) (*imessage.CreateGroupResponse, error) {
		calls++
		return &imessage.CreateGroupResponse{GUID: "SMS;+;sms-group", ThreadID: "sms-thread"}, nil
	}}
	mx := groupTestHandler(api)
	portal := addGroupTestPortal(t, mx, "SMS;+;sms-group")
	portal.MXID = ""
	portal.Name = "unpersisted metadata"
	as := groupTestAppservice(t, mx, func(*http.Request) (*http.Response, error) {
		matrixCalls++
		return &http.Response{StatusCode: http.StatusForbidden, Body: io.NopCloser(strings.NewReader(`{"errcode":"M_FORBIDDEN","error":"synthetic registration failure"}`)), Header: make(http.Header)}, nil
	})
	mx.bridge.Bot = as.BotIntent()
	req := groupTestRequest()
	req.Users[1], req.AllowSMS = "SMS;-;+12025550102", true
	for i := 0; i < 2; i++ {
		if _, err := mx.CreateGroup(req); err == nil || !strings.Contains(err.Error(), "failed to create Matrix room") || !strings.Contains(err.Error(), "may already exist") {
			t.Fatalf("missing Matrix failure uncertainty: %v", err)
		}
	}
	stored := mx.bridge.DB.Portal.GetByGUID(portal.GUID)
	if calls != 1 || matrixCalls != 1 || portal.ThreadID != "sms-thread" || stored.ThreadID != "sms-thread" || stored.Name != "" || stored.MXID != "!test:example.com" {
		t.Fatalf("Matrix failure retried, lost known identity, or overwrote unrelated metadata: create=%d matrix=%d", calls, matrixCalls)
	}
}

type incomingGroupTestLogger struct {
	maulogger.Logger
	entered chan struct{}
	once    sync.Once
}

func (log *incomingGroupTestLogger) Debugln(parts ...interface{}) {
	if len(parts) == 1 && parts[0] == "Received incoming message" {
		log.once.Do(func() { close(log.entered) })
	}
}

func waitGroupTestSignal(t *testing.T, signal <-chan struct{}) {
	t.Helper()
	select {
	case <-signal:
	case <-time.After(5 * time.Second):
		t.Fatal("concurrent group handoff did not complete")
	}
}

func TestCreateGroupConcurrentIncomingMessage(t *testing.T) {
	for _, echoFirst := range []bool{false, true} {
		t.Run(fmt.Sprintf("echo-first=%t", echoFirst), func(t *testing.T) {
			const guid = "iMessage;+;concurrent-group"
			submitted, respond, responded := make(chan struct{}), make(chan struct{}), make(chan struct{})
			registering, register := make(chan struct{}), make(chan struct{})
			var registrationOnce sync.Once
			var creates atomic.Int32
			api := &messageGroupTestAPI{
				groupTestAPI: &groupTestAPI{chat: func(chatGUID, threadID string) (*imessage.ChatInfo, error) {
					if chatGUID != guid || (threadID != "response-thread" && threadID != "echo-thread") {
						t.Errorf("lost known identity before chat lookup: %q %q", chatGUID, threadID)
					}
					return &imessage.ChatInfo{Identifier: imessage.ParseIdentifier(guid), ThreadID: "echo-thread"}, nil
				}},
				createMessage: func([]string, string) (*imessage.CreateGroupResponse, error) {
					close(submitted)
					if echoFirst {
						<-respond
					}
					close(responded)
					return &imessage.CreateGroupResponse{GUID: guid, ThreadID: "response-thread"}, nil
				},
			}
			mx := groupTestHandler(api)
			portal := addGroupTestPortal(t, mx, guid)
			portal.MXID = ""
			portal.Update(nil)
			portal.Messages = make(chan *imessage.Message, 1)
			portal.ReadReceipts = make(chan *imessage.ReadReceipt, 1)
			portal.MessageStatuses = make(chan *imessage.SendMessageStatus, 1)
			mx.bridge.portalsByMXID = make(map[id.RoomID]*Portal)
			mx.bridge.Config.BaseConfig = &mx.bridge.Bridge.Config
			mx.bridge.Config.Homeserver.Software = bridgeconfig.SoftwareHungry
			mx.bridge.Config.IMessage.Platform = "bluebubbles"
			mx.bridge.user = &User{User: &database.User{MXID: "@test:example.com"}, bridge: mx.bridge}
			mx.bridge.StateStore = sqlstatestore.NewSQLStateStore(mx.bridge.DB.Database, dbutil.NoopLogger, false)
			if err := mx.bridge.StateStore.Upgrade(); err != nil {
				t.Fatal(err)
			}
			as := groupTestAppservice(t, mx, func(req *http.Request) (*http.Response, error) {
				body := `{}`
				if strings.HasSuffix(req.URL.Path, "/register") {
					registrationOnce.Do(func() { close(registering) })
					<-register
					body = `{"user_id":"@testbot:example.com"}`
				} else if strings.HasSuffix(req.URL.Path, "/createRoom") {
					creates.Add(1)
					body = `{"room_id":"!created:example.com"}`
				}
				return &http.Response{StatusCode: http.StatusOK, Body: io.NopCloser(strings.NewReader(body)), Header: make(http.Header)}, nil
			})
			mx.bridge.Bot = as.BotIntent()
			entered := make(chan struct{})
			imh := &iMessageHandler{bridge: mx.bridge, log: &incomingGroupTestLogger{Logger: mx.log, entered: entered}}
			groupDone, echoDone := make(chan struct{}), make(chan struct{})
			var response *StartDMResponse
			var groupErr error
			go func() {
				defer close(groupDone)
				response, groupErr = mx.CreateGroup(groupTestRequest())
			}()
			waitGroupTestSignal(t, submitted)
			if !echoFirst {
				waitGroupTestSignal(t, registering)
			}
			go func() {
				defer close(echoDone)
				imh.HandleMessage(&imessage.Message{GUID: "synthetic-message", ChatGUID: guid, ThreadID: "echo-thread"})
			}()
			waitGroupTestSignal(t, entered)
			if echoFirst {
				waitGroupTestSignal(t, registering)
				close(respond)
				waitGroupTestSignal(t, responded)
			}
			var incoming sync.WaitGroup
			started := make(chan struct{}, 4)
			for _, handle := range []func(){
				func() { imh.HandleMessageStatus(&imessage.SendMessageStatus{ChatGUID: guid}) },
				func() { imh.HandleReadReceipt(&imessage.ReadReceipt{ChatGUID: guid}) },
				func() { imh.HandleTypingNotification(&imessage.TypingNotification{ChatGUID: guid}) },
				func() {
					imh.HandleChat(&imessage.ChatInfo{JSONChatGUID: guid, ThreadID: "echo-thread", NoCreateRoom: true})
				},
			} {
				incoming.Add(1)
				go func(handle func()) { defer incoming.Done(); started <- struct{}{}; handle() }(handle)
			}
			for i := 0; i < 4; i++ {
				<-started
			}
			close(register)
			waitGroupTestSignal(t, groupDone)
			waitGroupTestSignal(t, echoDone)
			incoming.Wait()
			if groupErr != nil || response == nil || response.RoomID != "!created:example.com" || creates.Load() != 1 || len(portal.Messages) != 1 || len(portal.ReadReceipts) != 1 || len(portal.MessageStatuses) != 1 {
				t.Fatalf("concurrent handoff failed: response=%#v error=%v room creates=%d", response, groupErr, creates.Load())
			}
			stored := mx.bridge.DB.Portal.GetByGUID(guid)
			if portal.ThreadID != "echo-thread" || stored.ThreadID != "echo-thread" || stored.MXID != response.RoomID {
				t.Fatal("echo identity or created room was not retained")
			}
			mx.bridge.portalsLock.Lock()
			indexed := mx.bridge.portalsByMXID[response.RoomID]
			mx.bridge.portalsLock.Unlock()
			if indexed != portal {
				t.Fatal("created room was not indexed after releasing the creation lock")
			}
		})
	}
}

func TestCreateGroupInflightAndFailureReplay(t *testing.T) {
	var calls atomic.Int32
	started, release := make(chan struct{}), make(chan struct{})
	sendErr := errors.New("synthetic timeout")
	api := &messageGroupTestAPI{groupTestAPI: &groupTestAPI{}, createMessage: func([]string, string) (*imessage.CreateGroupResponse, error) {
		if calls.Add(1) == 1 {
			close(started)
		}
		<-release
		return nil, sendErr
	}}
	mx := groupTestHandler(api)
	req := groupTestRequest()
	const count = 12
	errs := make(chan error, count)
	go func() { _, err := mx.CreateGroup(req); errs <- err }()
	<-started
	changed := req
	changed.AllowSMS = true
	if _, err := mx.CreateGroup(changed); err == nil || !strings.Contains(err.Error(), "different group request") {
		t.Fatalf("inflight mismatch was not rejected: %v", err)
	}
	var wg sync.WaitGroup
	for i := 1; i < count; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := mx.CreateGroup(req)
			errs <- err
		}()
	}
	close(release)
	wg.Wait()
	var first error
	for i := 0; i < count; i++ {
		err := <-errs
		if !errors.Is(err, sendErr) || !strings.Contains(err.Error(), "may already exist") || !strings.Contains(err.Error(), "do not repeat blindly") {
			t.Fatalf("missing uncertainty: %v", err)
		}
		if first == nil {
			first = err
		} else if err != first {
			t.Fatal("duplicate did not retain the same error")
		}
	}
	if _, err := mx.CreateGroup(req); err != first || calls.Load() != 1 {
		t.Fatalf("completed failure retried: %v, calls=%d", err, calls.Load())
	}
	// Identical text/recipients are allowed under a different stable transaction ID.
	req.TransactionID += "-different"
	_, _ = mx.CreateGroup(req)
	if calls.Load() != 2 {
		t.Fatal("different transaction IDs must not deduplicate by content")
	}
}

func TestCreateGroupCacheCapacityAndExpiry(t *testing.T) {
	var calls int
	api := &messageGroupTestAPI{groupTestAPI: &groupTestAPI{}, createMessage: func([]string, string) (*imessage.CreateGroupResponse, error) {
		calls++
		return nil, errors.New("synthetic failure")
	}}
	mx := groupTestHandler(api)
	req := groupTestRequest()
	for i := 0; i < 256; i++ {
		req.TransactionID = fmt.Sprintf("txn-%d", i)
		_, _ = mx.CreateGroup(req)
	}
	req.TransactionID = "new"
	if _, err := mx.CreateGroup(req); err == nil || !strings.Contains(err.Error(), "cache is full") || calls != 256 {
		t.Fatalf("protected outcomes were evicted: %v, calls=%d", err, calls)
	}
	req.TransactionID = "txn-0"
	_, _ = mx.CreateGroup(req)
	if calls != 256 {
		t.Fatal("full cache did not retain existing replay")
	}
	mx.groupCreates["txn-0"].expiresAt = time.Now().Add(-time.Second)
	req.TransactionID = "new"
	_, _ = mx.CreateGroup(req)
	if calls != 257 || len(mx.groupCreates) != 256 {
		t.Fatal("expired result was not pruned")
	}
	// Inflight entries have no expiry and must not be discarded to make room.
	mx.groupCreates["new"].expiresAt = time.Time{}
	req.TransactionID = "another"
	if _, err := mx.CreateGroup(req); err == nil || calls != 257 {
		t.Fatal("inflight entry was evicted")
	}
}

func TestCreateGroupValidation(t *testing.T) {
	for _, tc := range []struct {
		name, want string
		change     func(*CreateGroupRequest)
		resolve    func(string) (string, error)
	}{
		{"no transaction", "transaction_id", func(req *CreateGroupRequest) { req.TransactionID = "" }, nil},
		{"blank transaction", "transaction_id", func(req *CreateGroupRequest) { req.TransactionID = " \t" }, nil},
		{"no message", "nonempty first message", func(req *CreateGroupRequest) { req.Message = "" }, nil},
		{"blank message", "nonempty first message", func(req *CreateGroupRequest) { req.Message = " \n" }, nil},
		{"one recipient", "2–31", func(req *CreateGroupRequest) { req.Users = req.Users[:1] }, nil},
		{"too many", "2–31", func(req *CreateGroupRequest) { req.Users = make([]string, 32) }, nil},
		{"duplicate", "distinct", func(req *CreateGroupRequest) { req.Users[1] = req.Users[0] }, nil},
		{"normalized duplicate", "distinct", nil, func(string) (string, error) { return "iMessage;-;+12025550101", nil }},
		{"email case duplicate", "distinct", func(req *CreateGroupRequest) { req.Users = []string{"One@example.com", "one@example.com"} }, nil},
		{"sms consent", "allow_sms", func(req *CreateGroupRequest) { req.Users[1] = "SMS;-;+12025550102" }, nil},
		{"rcs", "RCS", func(req *CreateGroupRequest) { req.Users[1] = "RCS;-;+12025550102" }, nil},
		{"email downgrade", "email addresses cannot", func(req *CreateGroupRequest) {
			req.AllowSMS = true
			req.Users = []string{"one@example.com", "SMS;-;+12025550102"}
		}, nil},
		{"group participant", "direct identifier", func(req *CreateGroupRequest) { req.Users[1] = "iMessage;+;existing" }, nil},
		{"malformed", "direct identifier", nil, func(string) (string, error) { return "broken", nil }},
		{"unknown service", "unsupported service", nil, func(string) (string, error) { return "any;-;+12025550101", nil }},
		{"resolution failure", "failed to resolve group recipient 1", nil, func(string) (string, error) { return "", imessage.ErrIdentifierResolution }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var calls int
			api := &messageGroupTestAPI{groupTestAPI: &groupTestAPI{resolve: tc.resolve}, createMessage: func([]string, string) (*imessage.CreateGroupResponse, error) {
				calls++
				return nil, errors.New("unexpected send")
			}}
			mx := groupTestHandler(api)
			req := groupTestRequest()
			if tc.change != nil {
				tc.change(&req)
			}
			if _, err := mx.CreateGroup(req); err == nil || !strings.Contains(err.Error(), tc.want) || calls != 0 {
				t.Fatalf("expected %q without sending, got %v calls=%d", tc.want, err, calls)
			}
		})
	}
}

func TestCreateGroupRejectsUncertainResult(t *testing.T) {
	for _, guid := range []string{"", "broken", "iMessage;-;+12025550101", "iMessage;+;", "iMessage;+;id;extra", "SMS;+;wrong-service", "RCS;+;wrong-service", "any;+;unknown-service"} {
		t.Run(guid, func(t *testing.T) {
			var calls int
			api := &messageGroupTestAPI{groupTestAPI: &groupTestAPI{}, createMessage: func([]string, string) (*imessage.CreateGroupResponse, error) {
				calls++
				if guid == "" {
					return nil, nil
				}
				return &imessage.CreateGroupResponse{GUID: guid}, nil
			}}
			mx := groupTestHandler(api)
			for i := 0; i < 2; i++ {
				if _, err := mx.CreateGroup(groupTestRequest()); err == nil || !strings.Contains(err.Error(), "may already exist") {
					t.Fatalf("invalid create result was not uncertain: %v", err)
				}
			}
			if calls != 1 {
				t.Fatal("uncertain result retried")
			}
		})
	}
}

func TestStartChatForceAndGUIDValidation(t *testing.T) {
	for _, tc := range []struct {
		name, input, resolved string
		resolveErr            error
		wantSuccess           bool
	}{
		{"hard failure", "iMessage;-;+12025550101", "", fmt.Errorf("wrapped: %w", imessage.ErrIdentifierResolution), false},
		{"legacy force", "iMessage;-;+12025550101", "", errors.New("legacy resolution failure"), true},
		{"malformed force", "broken", "", errors.New("legacy resolution failure"), false},
		{"malformed resolved", "anything", "broken", nil, false},
		{"extra components", "anything", "iMessage;-;+12025550101;extra", nil, false},
		{"group resolved", "anything", "iMessage;+;group", nil, false},
		{"sms email", "anything", "SMS;-;one@example.com", nil, false},
		{"valid resolved", "anything", "iMessage;-;+12025550101", nil, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			mx := groupTestHandler(&groupTestAPI{resolve: func(string) (string, error) { return tc.resolved, tc.resolveErr }})
			mx.bridge.portalsByGUID["iMessage;-;+12025550101"] = &Portal{Portal: &database.Portal{MXID: "!existing:example.com"}}
			resp, err := mx.StartChat(StartDMRequest{Identifier: tc.input, Force: true, ActuallyStart: true})
			if (err == nil) != tc.wantSuccess {
				t.Fatalf("unexpected force outcome: %#v %v", resp, err)
			}
			if errors.Is(tc.resolveErr, imessage.ErrIdentifierResolution) && !errors.Is(err, imessage.ErrIdentifierResolution) {
				t.Fatal("hard resolution error was bypassed")
			}
		})
	}
}

type matrixTestTransport func(*http.Request) (*http.Response, error)

func (fn matrixTestTransport) RoundTrip(req *http.Request) (*http.Response, error) { return fn(req) }

func groupTestAppservice(t *testing.T, mx *WebsocketCommandHandler, transport matrixTestTransport) *appservice.AppService {
	t.Helper()
	as := appservice.Create()
	as.HomeserverDomain = "example.com"
	as.Registration = &appservice.Registration{SenderLocalpart: "testbot", AppToken: "synthetic-test-token"}
	if err := as.SetHomeserverURL("https://matrix.example.com"); err != nil {
		t.Fatal(err)
	}
	as.HTTPClient.Transport = transport
	mx.bridge.AS = as
	return as
}

type telemetryTestLogger struct {
	maulogger.Logger
	done chan struct{}
}

func (log *telemetryTestLogger) Debugln(...interface{})          { log.done <- struct{}{} }
func (log *telemetryTestLogger) Errorfln(string, ...interface{}) { log.done <- struct{}{} }

func TestResolveIdentifierTelemetryIsCategorical(t *testing.T) {
	previous := Segment
	t.Cleanup(func() { Segment = previous })
	done := make(chan struct{}, 1)
	requests := make(chan map[string]any, 1)
	Segment = SegmentClient{
		key: "synthetic-test-key", userID: "test-user", log: &telemetryTestLogger{done: done},
		client: http.Client{Transport: matrixTestTransport(func(req *http.Request) (*http.Response, error) {
			var payload map[string]any
			if err := json.NewDecoder(req.Body).Decode(&payload); err != nil {
				return nil, err
			}
			requests <- payload
			return &http.Response{StatusCode: http.StatusOK, Body: io.NopCloser(strings.NewReader("{}")), Header: make(http.Header)}, nil
		})},
	}
	mx := groupTestHandler(nil)
	mx.bridge.Config.HackyStartupTest.Identifier = "+12025550101"
	for _, tc := range []struct{ identifier, category, startup string }{
		{"+12025550101", "phone", "true"}, {"tel:+12025550102", "phone", "false"},
		{"+1 (202) 555-0102", "phone", "false"},
		{"iMessage;-;+12025550102", "phone", "false"}, {"one@example.com", "email", "false"},
		{"iMessage;-;one@example.com", "email", "false"}, {"not-an-address", "unknown", "false"},
	} {
		mx.trackResolveIdentifier(true, tc.identifier, "success")
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Fatal("fake telemetry request did not finish")
		}
		payload := <-requests
		props := payload["properties"].(map[string]any)
		if props["identifier_type"] != tc.category || props["is_startup_target"] != tc.startup || props["status"] != "success" {
			t.Fatalf("wrong categorical telemetry: %#v", props)
		}
		if len(props) != 4 || props["tmp_identifier"] != nil || payload["event"] != "iMC resolve identifier" {
			t.Fatalf("unexpected telemetry fields: %#v", payload)
		}
		encoded, _ := json.Marshal(payload)
		if strings.Contains(string(encoded), tc.identifier) {
			t.Fatal("telemetry contains the raw identifier")
		}
	}
}

func TestValidChatGUID(t *testing.T) {
	for _, guid := range []string{"", "broken", "iMessage;-", "iMessage;-;", "iMessage;-;a;extra", "Other;-;a", "iMessage;?;a", "iMessage;-;\n"} {
		if validChatGUID(guid, false) || validChatGUID(guid, true) {
			t.Errorf("accepted invalid GUID %q", guid)
		}
	}
	for _, service := range []string{"iMessage", "SMS", "RCS", "any"} {
		if !validChatGUID(service+";-;local", false) || !validChatGUID(service+";+;local", true) {
			t.Errorf("rejected valid service %s", service)
		}
	}
	for _, number := range []string{"", "+", "++1", "1+2", "１２３", "one@example.com"} {
		if isNumber(number) {
			t.Errorf("accepted nonnumeric identifier %q", number)
		}
	}
	for _, number := range []string{"123", "+12025550101"} {
		if !isNumber(number) {
			t.Errorf("rejected numeric identifier %q", number)
		}
	}
}
