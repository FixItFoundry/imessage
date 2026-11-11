package bluebubbles

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/rs/zerolog"
	"go.mau.fi/mautrix-imessage/imessage"
)

type syntheticBridge struct {
	imessage.Bridge
	config imessage.PlatformConfig
	log    zerolog.Logger
}

func (b *syntheticBridge) GetConnectorConfig() *imessage.PlatformConfig { return &b.config }
func (b *syntheticBridge) GetZLog() *zerolog.Logger                     { return &b.log }

func testConnector(t *testing.T, handler http.HandlerFunc) *blueBubbles {
	t.Helper()
	server := httptest.NewServer(handler)
	t.Cleanup(server.Close)
	b := &syntheticBridge{config: imessage.PlatformConfig{
		BlueBubblesURL: server.URL, BlueBubblesPassword: "synthetic-secret&+?",
	}, log: zerolog.New(io.Discard)}
	api, err := NewBlueBubblesConnector(b)
	if err != nil {
		t.Fatal(err)
	}
	return api.(*blueBubbles)
}

func jsonReply(w http.ResponseWriter, value string) {
	w.Header().Set("Content-Type", "application/json")
	_, _ = io.WriteString(w, value)
}

func TestContactCacheRefreshFailureAndCooldown(t *testing.T) {
	var requests int
	fail := false
	empty := false
	bb := testConnector(t, func(w http.ResponseWriter, r *http.Request) {
		requests++
		if r.URL.Path != "/api/v1/contact" || r.URL.Query().Get("extraProperties") != "avatar" {
			t.Error("contact request parameters changed")
		}
		if fail {
			w.WriteHeader(http.StatusServiceUnavailable)
			jsonReply(w, `{"status":503,"message":"synthetic upstream body"}`)
		} else if empty {
			jsonReply(w, `{"status":200,"data":[]}`)
		} else {
			jsonReply(w, `{"status":200,"data":[{"id":"synthetic-contact","firstName":"Synthetic","phoneNumbers":[{"address":"+12025550101"}],"emails":[{"address":"Synthetic@Example.test"}]}]}`)
		}
	})
	contacts, err := bb.GetContactList()
	if err != nil || len(contacts) != 1 || requests != 1 {
		t.Fatalf("initial refresh: count=%d requests=%d err=%v", len(contacts), requests, err)
	}
	if _, err = bb.SearchContactList("Synthetic"); err != nil || requests != 1 {
		t.Fatal("fresh snapshot was not reused")
	}
	if _, err = bb.GetContactInfo("synthetic@example.test"); err != nil {
		t.Fatal("case-insensitive email contact lookup failed")
	}
	if err = bb.RefreshContactList(); err != nil || requests != 2 {
		t.Fatal("explicit refresh did not fetch a fresh snapshot")
	}
	bb.contactsLock.Lock()
	bb.contactsLastRefresh = time.Now().Add(-6 * time.Minute)
	bb.contactsLock.Unlock()
	fail = true
	if _, err = bb.GetContactList(); err == nil || requests != 3 {
		t.Fatal("stale refresh failure was hidden")
	}
	if _, err = bb.SearchContactList("Synthetic"); err == nil || requests != 3 {
		t.Fatal("failed search must be visible and reuse the failure cooldown")
	}
	if contact, err := bb.GetContactInfo("+12025550101"); err != nil || contact.FirstName != "Synthetic" || requests != 3 {
		t.Fatal("contact info did not retain the previous successful snapshot")
	}
	if err = bb.RefreshContactList(); err == nil || requests != 4 {
		t.Fatal("explicit refresh did not bypass the failure cooldown")
	}
	bb.contactsLock.Lock()
	bb.contactsLastFailure = time.Now().Add(-31 * time.Second)
	bb.contactsLock.Unlock()
	fail, empty = false, true
	contacts, err = bb.GetContactList()
	if err != nil || len(contacts) != 0 || requests != 5 {
		t.Fatal("successful empty response did not replace the previous snapshot")
	}
}

func TestContactCacheWithoutSuccessfulSnapshot(t *testing.T) {
	for _, body := range []string{`{"status":500}`, `{"status":200}`, `{"status":200,"data":null}`, `not JSON`} {
		t.Run(body, func(t *testing.T) {
			var requests int
			bb := testConnector(t, func(w http.ResponseWriter, r *http.Request) {
				requests++
				jsonReply(w, body)
			})
			if _, err := bb.GetContactInfo("+12025550101"); err == nil {
				t.Fatal("missing successful snapshot must not become an empty success")
			}
			if _, err := bb.GetContactList(); err == nil || requests != 1 {
				t.Fatal("initial failure cooldown was not shared across cache callers")
			}
		})
	}
}

func TestContactCacheConcurrentReadersAndRefresh(t *testing.T) {
	var requests atomic.Int32
	bb := testConnector(t, func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		jsonReply(w, `{"status":200,"data":[{"id":42,"firstName":"Synthetic","phoneNumbers":[{"address":"+12025550101"}]}]}`)
	})
	var wg sync.WaitGroup
	for i := 0; i < 24; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if _, err := bb.GetContactList(); err != nil {
				t.Error(err)
			}
		}()
	}
	wg.Wait()
	if requests.Load() != 1 {
		t.Fatal("concurrent implicit requests were not coalesced")
	}
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			var err error
			switch i % 4 {
			case 0:
				err = bb.RefreshContactList()
			case 1:
				_, err = bb.SearchContactList("Synthetic")
			case 2:
				_, err = bb.GetContactList()
			case 3:
				_, err = bb.GetContactInfo("+12025550101")
			}
			if err != nil {
				t.Error(err)
			}
		}(i)
	}
	wg.Wait()
}

func TestContactConversionAndPrivacy(t *testing.T) {
	var logs bytes.Buffer
	bb := &blueBubbles{log: zerolog.New(&logs)}
	badAvatar := "not base64!"
	for _, avatar := range []*string{nil, &badAvatar} {
		contact, err := bb.convertBBContactToiMessageContact(&Contact{ID: "synthetic-id", FirstName: "SyntheticName", Avatar: avatar})
		if err != nil || contact.FirstName != "SyntheticName" || len(contact.Avatar) != 0 {
			t.Fatal("missing/malformed avatar lost contact metadata")
		}
	}
	for _, contact := range []*Contact{nil, {ID: []string{"synthetic-id"}}} {
		if _, err := bb.convertBBContactToiMessageContact(contact); err == nil {
			t.Fatal("malformed contact conversion must return an error")
		}
	}
	if strings.Contains(logs.String(), "SyntheticName") || strings.Contains(logs.String(), "synthetic-id") || strings.Contains(logs.String(), badAvatar) {
		t.Fatal("contact metadata leaked into conversion logs")
	}
	bb = testConnector(t, func(w http.ResponseWriter, r *http.Request) {
		jsonReply(w, `{"status":200,"data":[{"id":{},"firstName":"Synthetic","phoneNumbers":[{"address":"+12025550101"}]}]}`)
	})
	if _, err := bb.GetContactList(); err == nil {
		t.Fatal("list ignored conversion failure")
	}
	if _, err := bb.SearchContactList("Synthetic"); err == nil {
		t.Fatal("search ignored conversion failure")
	}
	if _, err := bb.GetContactInfo("+12025550101"); err == nil {
		t.Fatal("contact lookup ignored conversion failure")
	}
}

func TestResolveLiveAvailability(t *testing.T) {
	for _, tc := range []struct {
		name, input, body, want string
		httpStatus              int
	}{
		{"phone", "+1 (202) 555-0101", `{"status":200,"data":{"available":true}}`, "iMessage;-;+12025550101", 200},
		{"email", "Synthetic@Example.TEST", `{"status":200,"data":{"available":true}}`, "iMessage;-;synthetic@example.test", 200},
		{"supported email characters", "Synthetic_123.Name-Tag@Example-123.TEST", `{"status":200,"data":{"available":true}}`, "iMessage;-;synthetic_123.name-tag@example-123.test", 200},
		{"phone unavailable", "+12025550101", `{"status":200,"data":{"available":false}}`, "SMS;-;+12025550101", 200},
		{"email unavailable", "synthetic@example.test", `{"status":200,"data":{"available":false}}`, "", 200},
		{"explicit iMessage", "iMessage;-;+12025550101", `{"status":200,"data":{"available":true}}`, "iMessage;-;+12025550101", 200},
		{"explicit iMessage unavailable", "iMessage;-;+12025550101", `{"status":200,"data":{"available":false}}`, "", 200},
		{"missing data", "+12025550101", `{"status":200}`, "", 200},
		{"null data", "+12025550101", `{"status":200,"data":null}`, "", 200},
		{"missing available", "+12025550101", `{"status":200,"data":{}}`, "", 200},
		{"null available", "+12025550101", `{"status":200,"data":{"available":null}}`, "", 200},
		{"wrong available type", "+12025550101", `{"status":200,"data":{"available":"false"}}`, "", 200},
		{"envelope failure", "+12025550101", `{"status":500,"data":{"available":false}}`, "", 200},
		{"HTTP failure", "+12025550101", `{"status":200,"data":{"available":false}}`, "", 500},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var requests int
			bb := testConnector(t, func(w http.ResponseWriter, r *http.Request) {
				requests++
				if r.Method != http.MethodGet || r.URL.Path != "/api/v1/handle/availability/imessage" {
					t.Error("lookup must use only the availability GET endpoint")
				}
				_, address, err := normalizeRecipient(tc.input)
				if err != nil || r.URL.Query().Get("address") != address || r.URL.Query().Get("password") != "synthetic-secret&+?" {
					t.Error("availability query normalization/authentication changed")
				}
				w.WriteHeader(tc.httpStatus)
				jsonReply(w, tc.body)
			})
			bb.usingPrivateAPI = true
			got, err := bb.ResolveIdentifier(tc.input)
			if got != tc.want || (err == nil) != (tc.want != "") || requests != 1 {
				t.Fatalf("got=%q want=%q err=%v requests=%d", got, tc.want, err, requests)
			}
			if err != nil && !errors.Is(err, imessage.ErrIdentifierResolution) {
				t.Fatal("resolver failure can bypass the hard-resolution sentinel")
			}
			if err = bb.PrepareDM(got); err != nil || requests != 1 {
				t.Fatal("DM preparation must not send a probe message")
			}
		})
	}
}

func TestResolveValidationBeforeNetwork(t *testing.T) {
	bb := testConnector(t, func(w http.ResponseWriter, r *http.Request) {
		t.Error("invalid/explicit SMS resolution must not make network requests")
	})
	bb.usingPrivateAPI = true
	for _, input := range []string{
		"", "2025550101", "+02025550101", "+123456", "+1234567890123456", "+１２３４５６７８", "+1202\t5550101", "+12025550101\n",
		"+1.202.555.0101", "++12025550101", "+1\u00a02025550101", "Name <synthetic@example.test>", "synthetic@example.test;extra",
		"iMessage;-;+12025550101;extra", "iMessage;+;synthetic-group", "any;-;+12025550101", "SMS;-;synthetic@example.test", "RCS;-;synthetic@example.test",
	} {
		if _, err := bb.ResolveIdentifier(input); !errors.Is(err, imessage.ErrIdentifierResolution) {
			t.Fatalf("invalid input did not fail closed: %q", input)
		}
	}
	if got, err := bb.ResolveIdentifier("SMS;-;+1 (202) 555-0101"); err != nil || got != "SMS;-;+12025550101" {
		t.Fatal("explicit numeric SMS choice was not preserved")
	}
}

func TestResolveStoredIdentity(t *testing.T) {
	for _, tc := range []struct {
		input, storedService, storedAddress, want string
		private                                   bool
	}{
		{"+12025550101", "iMessage", "+1 (202) 555-0101", "iMessage;-;+12025550101", false},
		{"+12025550101", "SMS", "+12025550101", "SMS;-;+12025550101", false},
		{"synthetic_123.name-tag@example-123.test", "iMessage", "Synthetic_123.Name-Tag@Example-123.TEST", "iMessage;-;synthetic_123.name-tag@example-123.test", false},
		{"synthetictag@example.test", "iMessage", "synthetic+tag@example.test", "", false},
		{"RCS;-;+12025550101", "RCS", "+12025550101", "RCS;-;+12025550101", true},
		{"RCS;-;+12025550101", "SMS", "+12025550101", "", true},
		{"iMessage;-;+12025550101", "SMS", "+12025550101", "", false},
		{"+12025550101", "", "", "", false},
		{"+12025550101", "iMessage", "+12025550102", "", false},
		{"+12025550101", "arbitrary", "+12025550101", "", false},
		{"synthetic@example.test", "SMS", "synthetic@example.test", "", false},
	} {
		t.Run(tc.input+tc.storedService+tc.storedAddress, func(t *testing.T) {
			bb := testConnector(t, func(w http.ResponseWriter, r *http.Request) {
				_, address, _ := normalizeRecipient(tc.input)
				if r.Method != "GET" || r.URL.EscapedPath() != "/api/v1/handle/"+url.PathEscape(address) {
					t.Error("stored handle path was not correctly escaped")
				}
				body, _ := json.Marshal(ResolveIdentifierResponse{Status: 200, Data: Handle{Service: tc.storedService, Address: tc.storedAddress}})
				_, _ = w.Write(body)
			})
			bb.usingPrivateAPI = tc.private
			got, err := bb.ResolveIdentifier(tc.input)
			if got != tc.want || (err == nil) != (tc.want != "") {
				t.Fatalf("got=%q want=%q err=%v", got, tc.want, err)
			}
			if err != nil && !errors.Is(err, imessage.ErrIdentifierResolution) {
				t.Fatal("stored identity failure did not wrap sentinel")
			}
		})
	}
}

func TestRewriteableEmailsRejectedBeforeRequests(t *testing.T) {
	addresses := []string{"synthétic@example.test", "synthetic@exämple.test", "syntheticK@example.test", `"synthetic tag"@example.test`}
	for _, punctuation := range "+/!#$%&'*=?^`{|}~:;" {
		addresses = append(addresses, "synthetic"+string(punctuation)+"tag@example.test")
	}
	for _, address := range addresses {
		t.Run(address, func(t *testing.T) {
			var requests int
			bb := testConnector(t, func(w http.ResponseWriter, r *http.Request) {
				requests++
				jsonReply(w, `{"status":200,"data":{"available":true}}`)
			})
			for _, private := range []bool{false, true} {
				bb.usingPrivateAPI = private
				for _, input := range []string{address, "iMessage;-;" + address} {
					if got, err := bb.ResolveIdentifier(input); got != "" || !errors.Is(err, imessage.ErrIdentifierResolution) {
						t.Fatal("rewriteable email lookup did not fail closed")
					}
				}
			}
			bb.usingPrivateAPI = true
			for _, guids := range [][]string{
				{"iMessage;-;" + address, "iMessage;-;synthetic-other@example.test"},
				{"iMessage;-;synthetic-other@example.test", "iMessage;-;" + address},
			} {
				if _, err := bb.CreateGroupWithMessage(guids, "synthetic"); err == nil {
					t.Fatal("rewriteable group email was accepted")
				}
			}
			if requests != 0 {
				t.Fatalf("rejected recipients reached %d endpoints", requests)
			}
		})
	}
}

func TestRewrittenEmailCollisionRejectedBeforeRequests(t *testing.T) {
	var requests int
	bb := testConnector(t, func(w http.ResponseWriter, r *http.Request) {
		requests++
		jsonReply(w, `{"status":200,"data":{"available":true}}`)
	})
	bb.usingPrivateAPI = true
	// Both would become synthetictag@example.test on the server, not their intended mailboxes.
	guids := []string{"iMessage;-;synthetic+tag@example.test", "iMessage;-;synthetic/tag@example.test"}
	for _, guid := range guids {
		if _, err := bb.ResolveIdentifier(guid); !errors.Is(err, imessage.ErrIdentifierResolution) {
			t.Fatal("colliding rewriteable email lookup was accepted")
		}
	}
	if _, err := bb.CreateGroupWithMessage(guids, "synthetic"); err == nil || requests != 0 {
		t.Fatal("rewriteable email collision must be rejected without any HTTP request")
	}
}

func TestSupportedGroupEmailCharactersPreserved(t *testing.T) {
	var requests int
	bb := testConnector(t, func(w http.ResponseWriter, r *http.Request) {
		requests++
		var request CreateChatRequest
		if r.Method != "POST" || r.URL.Path != "/api/v1/chat/new" || json.NewDecoder(r.Body).Decode(&request) != nil || len(request.Addresses) != 2 {
			t.Error("supported email group request was not preserved")
			return
		}
		if request.Addresses[0] != "synthetic_123.name-tag@example-123.test" || request.Addresses[1] != "synthetic-other@example.test" || request.Service != "iMessage" {
			t.Error("supported email characters were changed instead of lowercased")
		}
		jsonReply(w, `{"status":200,"data":{"guid":"iMessage;+;synthetic-group"}}`)
	})
	bb.usingPrivateAPI = true
	_, err := bb.CreateGroupWithMessage([]string{"iMessage;-;Synthetic_123.Name-Tag@Example-123.TEST", "iMessage;-;Synthetic-Other@Example.TEST"}, "synthetic")
	if err != nil || requests != 1 {
		t.Fatal("safe email group must make exactly one creation request")
	}
}

func TestGroupCreationPayloadAndIncomingThread(t *testing.T) {
	for _, service := range []string{"iMessage", "SMS"} {
		t.Run(service, func(t *testing.T) {
			var posts int
			message := "  synthetic first message\nwith spacing  "
			bb := testConnector(t, func(w http.ResponseWriter, r *http.Request) {
				posts++
				if r.Method != "POST" || r.URL.Path != "/api/v1/chat/new" || r.Header.Get("Content-Type") != "application/json" || r.URL.Query().Get("password") != "synthetic-secret&+?" {
					t.Error("group endpoint/authentication changed")
				}
				var request map[string]any
				if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
					t.Fatal(err)
				}
				addresses, ok := request["addresses"].([]any)
				if len(request) != 4 || request["message"] != message || request["service"] != service || request["method"] != "private-api" || !ok || len(addresses) != 2 || addresses[0] != "+12025550101" || addresses[1] != "+12025550102" {
					t.Error("group creation payload does not match the contract")
				}
				jsonReply(w, fmt.Sprintf(`{"status":200,"data":{"guid":%q,"groupId":"synthetic-thread"}}`, service+";+;synthetic-group"))
			})
			bb.usingPrivateAPI = true
			var optional imessage.GroupChatWithMessageAPI = bb
			response, err := optional.CreateGroupWithMessage([]string{"iMessage;-;+1 (202) 555-0101", service + ";-;+12025550102"}, message)
			if err != nil || posts != 1 || response.GUID != service+";+;synthetic-group" || response.ThreadID != "synthetic-thread" {
				t.Fatalf("creation mapping failed: posts=%d err=%v", posts, err)
			}
			for _, reply := range []string{"", "synthetic-reply-before", "synthetic-reply-after"} {
				incoming, err := bb.convertBBMessageToiMessage(Message{Chats: []Chat{{GUID: response.GUID, GroupID: response.ThreadID}}, ThreadOriginatorGUID: reply})
				if err != nil || incoming.ThreadID != response.ThreadID || incoming.ReplyToGUID != reply {
					t.Fatal("reply identity replaced conversation identity")
				}
			}
		})
	}
}

func TestGroupCreationValidation(t *testing.T) {
	for _, tc := range []struct {
		name, message string
		guids         []string
		private       bool
	}{
		{"Private API required", "synthetic", []string{"SMS;-;+12025550101", "SMS;-;+12025550102"}, false},
		{"message required", " \t\n", []string{"SMS;-;+12025550101", "SMS;-;+12025550102"}, true},
		{"minimum", "synthetic", []string{"SMS;-;+12025550101"}, true},
		{"maximum", "synthetic", make([]string, 32), true},
		{"duplicate phone", "synthetic", []string{"iMessage;-;+12025550101", "SMS;-;+1 (202) 555-0101"}, true},
		{"duplicate email", "synthetic", []string{"iMessage;-;Synthetic@Example.test", "iMessage;-;synthetic@example.test"}, true},
		{"RCS", "synthetic", []string{"RCS;-;+12025550101", "SMS;-;+12025550102"}, true},
		{"group participant", "synthetic", []string{"iMessage;+;synthetic", "SMS;-;+12025550102"}, true},
		{"unwrapped participant", "synthetic", []string{"+12025550101", "SMS;-;+12025550102"}, true},
		{"email downgrade", "synthetic", []string{"iMessage;-;synthetic@example.test", "SMS;-;+12025550102"}, true},
		{"invalid number", "synthetic", []string{"SMS;-;2025550101", "SMS;-;+12025550102"}, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			bb := testConnector(t, func(w http.ResponseWriter, r *http.Request) {
				t.Error("invalid group must not make any HTTP request")
			})
			bb.usingPrivateAPI = tc.private
			if _, err := bb.CreateGroupWithMessage(tc.guids, tc.message); err == nil {
				t.Fatal("invalid group was accepted")
			}
		})
	}
	bb := &blueBubbles{}
	if _, err := bb.CreateGroup(nil); !errors.Is(err, errors.ErrUnsupported) || !strings.Contains(err.Error(), "first message") {
		t.Fatal("legacy creation must explain that a first message is required")
	}
}

func TestGroupCreationUncertainOutcomes(t *testing.T) {
	for _, tc := range []struct {
		name, body string
		status     int
	}{
		{"HTTP failure", `{"status":200,"data":{"guid":"iMessage;+;synthetic"}}`, 500},
		{"envelope failure", `{"status":500,"message":"synthetic private body"}`, 200},
		{"malformed JSON", `not JSON`, 200},
		{"null data", `{"status":200,"data":null}`, 200},
		{"missing data", `{"status":200}`, 200},
		{"direct result", `{"status":200,"data":{"guid":"iMessage;-;+12025550101"}}`, 200},
		{"malformed result", `{"status":200,"data":{"guid":"malformed"}}`, 200},
		{"empty group", `{"status":200,"data":{"guid":"iMessage;+;"}}`, 200},
		{"extra component", `{"status":200,"data":{"guid":"iMessage;+;synthetic;extra"}}`, 200},
		{"unknown service", `{"status":200,"data":{"guid":"unknown;+;synthetic"}}`, 200},
		{"unexpected service", `{"status":200,"data":{"guid":"iMessage;+;synthetic"}}`, 200},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var posts int
			bb := testConnector(t, func(w http.ResponseWriter, r *http.Request) {
				posts++
				w.WriteHeader(tc.status)
				jsonReply(w, tc.body)
			})
			bb.usingPrivateAPI = true
			_, err := bb.CreateGroupWithMessage([]string{"SMS;-;+12025550101", "SMS;-;+12025550102"}, "synthetic")
			if posts != 1 || err == nil || !strings.Contains(err.Error(), "may already exist") || !strings.Contains(err.Error(), "do not repeat blindly") || strings.Contains(err.Error(), "synthetic private body") {
				t.Fatalf("uncertain creation was retried or not explained: posts=%d err=%v", posts, err)
			}
		})
	}
}

func TestGroupCreationDoesNotFollowRedirect(t *testing.T) {
	var posts, redirected int
	bb := testConnector(t, func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/redirected" {
			redirected++
			jsonReply(w, `{"status":200}`)
			return
		}
		posts++
		http.Redirect(w, r, "/redirected?synthetic-location-secret=hidden", http.StatusTemporaryRedirect)
	})
	bb.usingPrivateAPI = true
	_, err := bb.CreateGroupWithMessage([]string{"SMS;-;+12025550101", "SMS;-;+12025550102"}, "synthetic")
	if err == nil || posts != 1 || redirected != 0 {
		t.Fatal("redirect replayed a potentially submitted group creation")
	}
	if strings.Contains(err.Error(), "synthetic-location-secret") || strings.Contains(err.Error(), "redirected") {
		t.Fatal("redirect headers/body leaked into the returned error")
	}
}

func TestRecipientAndGroupSizeBoundaries(t *testing.T) {
	for _, address := range []string{"+1234567", "+123456789012345"} {
		if _, got, err := normalizeRecipient(address); err != nil || got != address {
			t.Fatal("valid international number boundary was rejected")
		}
	}
	var posts int
	bb := testConnector(t, func(w http.ResponseWriter, r *http.Request) {
		posts++
		var request CreateChatRequest
		if json.NewDecoder(r.Body).Decode(&request) != nil || len(request.Addresses) != 31 {
			t.Error("maximum valid group size was not preserved")
		}
		jsonReply(w, `{"status":200,"data":{"guid":"iMessage;+;synthetic-group"}}`)
	})
	bb.usingPrivateAPI = true
	guids := make([]string, 31)
	for i := range guids {
		guids[i] = fmt.Sprintf("iMessage;-;+1202555%04d", 100+i)
	}
	if _, err := bb.CreateGroupWithMessage(guids, "synthetic"); err != nil || posts != 1 {
		t.Fatal("31 distinct recipients must be accepted")
	}
}

func TestIncomingMissingOrInvalidChat(t *testing.T) {
	bb := &blueBubbles{log: zerolog.New(io.Discard)}
	for _, chats := range [][]Chat{nil, {{GUID: "malformed"}}, {{GUID: "iMessage;+;"}}, {{GUID: "iMessage;+;synthetic"}, {GUID: "SMS;+;synthetic"}}} {
		if _, err := bb.convertBBMessageToiMessage(Message{Chats: chats}); err == nil {
			t.Fatal("missing or invalid chat was accepted")
		}
	}
	if err := bb.chatRoomUpdate(json.RawMessage(`{}`), "synthetic"); err == nil {
		t.Fatal("missing chat update must fail without indexing an empty slice")
	}
	if _, err := bb.convertBBChatToiMessageChat(Chat{GUID: "malformed"}); err == nil {
		t.Fatal("malformed chat must not reach ParseIdentifier")
	}
}

type syntheticTransport func(*http.Request) (*http.Response, error)

func (f syntheticTransport) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

func TestRequestDeadlineAndCredentialSafeErrors(t *testing.T) {
	original := http.DefaultTransport
	t.Cleanup(func() { http.DefaultTransport = original })
	bb := testConnector(t, func(w http.ResponseWriter, r *http.Request) {
		t.Error("fake transport must not reach a network server")
	})
	var logs bytes.Buffer
	bb.log = zerolog.New(&logs)
	var requests int
	http.DefaultTransport = syntheticTransport(func(r *http.Request) (*http.Response, error) {
		requests++
		deadline, ok := r.Context().Deadline()
		if remaining := time.Until(deadline); !ok || remaining < 44*time.Second || remaining > 45*time.Second {
			t.Error("JSON requests must have a 45-second timeout")
		}
		return nil, &url.Error{Op: r.Method, URL: r.URL.String(), Err: errors.New("synthetic-secret&+? +12025550101 synthetic-body")}
	})
	bb.usingPrivateAPI = true
	_, resolveErr := bb.ResolveIdentifier("+12025550101")
	_, groupErr := bb.CreateGroupWithMessage([]string{"SMS;-;+12025550101", "SMS;-;+12025550102"}, "synthetic-body")
	if !errors.Is(resolveErr, imessage.ErrIdentifierResolution) || groupErr == nil || !strings.Contains(groupErr.Error(), "may already exist") || requests != 2 {
		t.Fatal("transport failures lost sentinel/uncertainty or retried a request")
	}
	for _, text := range []string{resolveErr.Error(), groupErr.Error(), logs.String()} {
		if strings.Contains(text, "synthetic-secret") || strings.Contains(text, "+12025550101") || strings.Contains(text, "synthetic-body") || strings.Contains(text, "password=") {
			t.Fatal("credential-bearing transport error leaked into errors/logging")
		}
	}
}

func TestSharedJSONHelpersAndExistingReadSend(t *testing.T) {
	var methods []string
	bb := testConnector(t, func(w http.ResponseWriter, r *http.Request) {
		methods = append(methods, r.Method)
		if r.URL.Query().Get("existing") != "preserved" || r.URL.Query().Get("password") != "synthetic-secret&+?" {
			t.Error("shared helper lost existing query/authentication")
		}
		switch r.URL.Path {
		case "/api/v1/message/synthetic-message":
			if r.URL.Query().Get("with") != "chats" {
				t.Error("message read query changed")
			}
			jsonReply(w, `{"status":200,"data":{"guid":"synthetic-message","chats":[{"guid":"iMessage;+;synthetic-group","groupId":"synthetic-thread"}],"threadOriginatorGuid":"synthetic-reply","handle":{"service":"iMessage","address":"+12025550101"}}}`)
		case "/api/v1/message/text":
			var request SendTextRequest
			if json.NewDecoder(r.Body).Decode(&request) != nil || request.Message != "synthetic text" || request.ChatGUID != "iMessage;+;synthetic-group" || request.SelectedMessageGUID != "synthetic-reply" || request.PartIndex != 2 || request.Method != "private-api" {
				t.Error("existing text send payload changed")
			}
			jsonReply(w, `{"status":200,"data":{"guid":"synthetic-sent","dateCreated":1700000000000,"handle":{"service":"iMessage"}}}`)
		default:
			jsonReply(w, `{"status":200}`)
		}
	})
	bb.bridge.GetConnectorConfig().BlueBubblesURL += "?existing=preserved"
	bb.usingPrivateAPI = true
	path := "/api/v1/chat/iMessage;-;synthetic%tag@example.test"
	parsed, err := url.Parse(bb.apiURL(path, nil))
	if err != nil || parsed.Path != path {
		t.Fatal("shared URL helper changed literal percent characters in existing paths")
	}
	read, err := bb.GetMessage("synthetic-message")
	if err != nil || read.ThreadID != "synthetic-thread" || read.ReplyToGUID != "synthetic-reply" {
		t.Fatal("existing message read regressed")
	}
	sent, err := bb.SendMessage("iMessage;+;synthetic-group", "synthetic text", "synthetic-reply", 2, nil, nil)
	if err != nil || sent.GUID != "synthetic-sent" || sent.Service != "iMessage" || sent.Time.UnixMilli() != 1700000000000 {
		t.Fatal("existing text send regressed")
	}
	if err := bb.SendTypingNotification("iMessage;+;synthetic-group", false); err != nil {
		t.Fatal("DELETE helper regressed")
	}
	if strings.Join(methods, ",") != "GET,POST,DELETE" {
		t.Fatal("shared helpers changed HTTP methods")
	}
}

func TestJSONHelpersRejectHTTPAndEnvelopeFailures(t *testing.T) {
	for _, method := range []string{"GET", "POST", "DELETE"} {
		for _, tc := range []struct {
			status int
			body   string
		}{
			{500, `{"status":200}`}, {200, `{"status":500}`}, {200, `{}`}, {200, `not JSON`}, {200, `{"status":"200"}`},
		} {
			t.Run(fmt.Sprint(method, tc.status, tc.body), func(t *testing.T) {
				bb := testConnector(t, func(w http.ResponseWriter, r *http.Request) {
					w.WriteHeader(tc.status)
					jsonReply(w, tc.body)
				})
				var target map[string]any
				var err error
				if method == "GET" {
					err = bb.apiGet("/synthetic", nil, &target)
				} else {
					err = bb.apiRequest(method, "/synthetic", nil, &target)
				}
				if err == nil {
					t.Fatal("HTTP/envelope/JSON failure was accepted")
				}
			})
		}
	}
}
