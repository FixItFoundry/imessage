package main

import (
	"errors"
	"io"
	"net/http"
	"reflect"
	"strings"
	"testing"

	"github.com/rs/zerolog"
	"go.mau.fi/mautrix-imessage/imessage"
	"maunium.net/go/mautrix/bridge/commands"
	"maunium.net/go/mautrix/event"
)

func TestParseCreateGroupArgs(t *testing.T) {
	for _, tc := range []struct {
		name, raw, message string
		allowSMS           bool
	}{
		{"plain", "+12025550101 +12025550102 -- hello world", "hello world", false},
		{"sms", "--allow-sms +12025550101 +12025550102 -- hello", "hello", true},
		{"preserve whitespace", " +12025550101\t+12025550102 --   hello\t world  \n", "  hello\t world  \n", false},
		{"newline separator", "+12025550101 +12025550102 --\n\nhello\n", "\nhello\n", false},
		{"unicode separator", "+12025550101 +12025550102 --\u2003\u2003hello", "\u2003hello", false},
		{"message delimiter", "+12025550101 +12025550102 -- hello -- world", "hello -- world", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			req, err := parseCreateGroupArgs(tc.raw)
			if err != nil {
				t.Fatal(err)
			}
			if req.Message != tc.message || req.AllowSMS != tc.allowSMS || !reflect.DeepEqual(req.Users, []string{"+12025550101", "+12025550102"}) {
				t.Fatalf("unexpected parsed request: %#v", req)
			}
		})
	}
}

func TestParseCreateGroupArgsRejectsInvalid(t *testing.T) {
	for _, raw := range []string{
		"", "+12025550101 +12025550102 hello", "+12025550101 +12025550102 --hello",
		"+12025550101 +12025550102 --", "+12025550101 +12025550102 -- \t\n",
		"-- hello", "+12025550101 -- hello",
		"+12025550101 --allow-sms +12025550102 -- hello",
		"--unknown +12025550101 +12025550102 -- hello",
		strings.Repeat("+12025550101 ", 32) + "-- hello",
	} {
		if _, err := parseCreateGroupArgs(raw); err == nil {
			t.Errorf("expected rejection for %q", raw)
		}
	}
}

func TestCreateGroupCommandUsesEventID(t *testing.T) {
	var sends, replies int
	api := &messageGroupTestAPI{groupTestAPI: &groupTestAPI{}, createMessage: func(_ []string, message string) (*imessage.CreateGroupResponse, error) {
		sends++
		if message != "  first message  " {
			t.Fatalf("command changed first message: %q", message)
		}
		return nil, errors.New("synthetic send failure")
	}}
	mx := groupTestHandler(api)
	mx.bridge.WebsocketHandler = mx
	mx.bridge.Bridge.Config.Bridge = &mx.bridge.Config.Bridge
	as := groupTestAppservice(t, mx, func(*http.Request) (*http.Response, error) {
		replies++
		return &http.Response{StatusCode: http.StatusOK, Body: io.NopCloser(strings.NewReader(`{"event_id":"$reply:example.com"}`)), Header: make(http.Header)}, nil
	})
	as.StateStore.SetMembership("!commands:example.com", as.BotMXID(), event.MembershipJoin)
	zlog := zerolog.Nop()
	ce := &WrappedCommandEvent{
		Event: &commands.Event{
			Bridge: &mx.bridge.Bridge, Bot: as.BotIntent(), RoomID: "!commands:example.com",
			EventID: "$command:example.com", ZLog: &zlog,
			RawArgs: "+12025550101 +12025550102 --   first message  ",
		},
		Bridge: mx.bridge,
	}
	fnCreateGroup(ce)
	fnCreateGroup(ce)
	if sends != 1 || replies != 2 || mx.groupCreates[ce.EventID.String()] == nil {
		t.Fatalf("command did not share event-ID replay protection: sends=%d replies=%d", sends, replies)
	}
}
