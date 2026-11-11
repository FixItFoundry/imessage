// mautrix-imessage - A Matrix-iMessage puppeting bridge.
// Copyright (C) 2022 Tulir Asokan
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program.  If not, see <https://www.gnu.org/licenses/>.

package main

import (
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unicode"

	"maunium.net/go/maulogger/v2"
	"maunium.net/go/mautrix"
	"maunium.net/go/mautrix/appservice"
	"maunium.net/go/mautrix/id"

	"go.mau.fi/mautrix-imessage/imessage"
)

const DefaultSyncProxyBackoff = 1 * time.Second
const MaxSyncProxyBackoff = 60 * time.Second

type WebsocketCommandHandler struct {
	bridge      *IMBridge
	log         maulogger.Logger
	errorTxnIDC *appservice.TransactionIDCache

	lastSyncProxyError time.Time
	syncProxyBackoff   time.Duration
	syncProxyWaiting   int64

	createRoomsForBackfillError error

	groupCreateLock sync.Mutex
	groupCreates    map[string]*groupCreateTransaction
}

type groupCreateTransaction struct {
	digest    [sha256.Size]byte
	done      chan struct{}
	expiresAt time.Time
	response  *StartDMResponse
	err       error
}

func NewWebsocketCommandHandler(br *IMBridge) *WebsocketCommandHandler {
	handler := &WebsocketCommandHandler{
		bridge:           br,
		log:              br.Log.Sub("MatrixWebsocket"),
		errorTxnIDC:      appservice.NewTransactionIDCache(8),
		syncProxyBackoff: DefaultSyncProxyBackoff,
	}
	br.AS.PrepareWebsocket()
	br.AS.SetWebsocketCommandHandler("ping", handler.handleWSPing)
	br.AS.SetWebsocketCommandHandler("syncproxy_error", handler.handleWSSyncProxyError)
	br.AS.SetWebsocketCommandHandler("create_group", handler.handleWSCreateGroup)
	br.AS.SetWebsocketCommandHandler("start_dm", handler.handleWSStartDM)
	br.AS.SetWebsocketCommandHandler("resolve_identifier", handler.handleWSStartDM)
	br.AS.SetWebsocketCommandHandler("list_contacts", handler.handleWSGetContacts)
	br.AS.SetWebsocketCommandHandler("upload_contacts", handler.handleWSUploadContacts)
	br.AS.SetWebsocketCommandHandler("edit_ghost", handler.handleWSEditGhost)
	br.AS.SetWebsocketCommandHandler("do_hacky_test", handler.handleWSHackyTest)
	br.AS.SetWebsocketCommandHandler("create_rooms_for_backfill", handler.handleCreateRoomsForBackfill)
	br.AS.SetWebsocketCommandHandler("get_room_info_for_backfill", handler.handleGetRoomInfoForBackfill)
	return handler
}

func (mx *WebsocketCommandHandler) handleWSHackyTest(cmd appservice.WebsocketCommand) (bool, any) {
	mx.log.Debugfln("Starting hacky test due to manual request")
	mx.bridge.hackyStartupTests(false, true)
	return true, nil
}

func (mx *WebsocketCommandHandler) handleWSPing(cmd appservice.WebsocketCommand) (bool, interface{}) {
	var status imessage.BridgeStatus

	if mx.bridge.latestState != nil {
		status = *mx.bridge.latestState
	} else {
		status = imessage.BridgeStatus{
			StateEvent: BridgeStatusConnected,
			Timestamp:  time.Now().Unix(),
			TTL:        600,
			Source:     "bridge",
		}
	}

	return true, &status
}

func (mx *WebsocketCommandHandler) handleWSSyncProxyError(cmd appservice.WebsocketCommand) (bool, interface{}) {
	var data mautrix.RespError
	err := json.Unmarshal(cmd.Data, &data)

	if err != nil {
		mx.log.Warnln("Failed to unmarshal syncproxy_error data:", err)
	} else if txnID, ok := data.ExtraData["txn_id"].(string); !ok {
		mx.log.Warnln("Got syncproxy_error data with no transaction ID")
	} else if mx.errorTxnIDC.IsProcessed(txnID) {
		mx.log.Debugln("Ignoring syncproxy_error with duplicate transaction ID", txnID)
	} else {
		go mx.HandleSyncProxyError(&data, nil)
		mx.errorTxnIDC.MarkProcessed(txnID)
	}

	return true, &data
}

type ProfileOverride struct {
	Displayname string `json:"displayname,omitempty"`
	PhotoURL    string `json:"photo_url,omitempty"`
}

type EditGhostRequest struct {
	UserID id.UserID `json:"user_id"`
	RoomID id.RoomID `json:"room_id"`
	Reset  bool      `json:"reset"`
	ProfileOverride
}

func (mx *WebsocketCommandHandler) handleWSEditGhost(cmd appservice.WebsocketCommand) (bool, interface{}) {
	var req EditGhostRequest
	if err := json.Unmarshal(cmd.Data, &req); err != nil {
		return false, fmt.Errorf("failed to parse request: %w", err)
	}
	var puppet *Puppet
	if req.UserID != "" {
		puppet = mx.bridge.GetPuppetByMXID(req.UserID)
		if puppet == nil {
			return false, fmt.Errorf("user is not a bridge ghost")
		}
	} else if req.RoomID != "" {
		portal := mx.bridge.GetPortalByMXID(req.RoomID)
		if portal == nil {
			return false, fmt.Errorf("unknown room ID provided")
		} else if !portal.IsPrivateChat() {
			return false, fmt.Errorf("provided room is not a direct chat")
		} else if puppet = portal.GetDMPuppet(); puppet == nil {
			return false, fmt.Errorf("unexpected error: private chat portal doesn't have ghost")
		}
	} else {
		return false, fmt.Errorf("neither room nor user ID were provided")
	}
	if req.Reset {
		puppet.log.Debugfln("Marking name as not overridden and resyncing profile")
		puppet.NameOverridden = false
		puppet.Update()
		puppet.Sync()
	} else {
		puppet.log.Debugfln("Updating profile with %+v", req.ProfileOverride)
		puppet.SyncWithProfileOverride(req.ProfileOverride)
	}
	return true, struct{}{}
}

type StartDMRequest struct {
	Identifier string `json:"identifier"`
	Force      bool   `json:"force"`
	ProfileOverride

	ActuallyStart bool `json:"-"`
}

type CreateGroupRequest struct {
	Users []string `json:"users"`

	AllowSMS      bool   `json:"allow_sms"`
	Message       string `json:"message,omitempty"`
	TransactionID string `json:"transaction_id,omitempty"`
}

type StartDMResponse struct {
	RoomID      id.RoomID `json:"room_id,omitempty"`
	GUID        string    `json:"guid"`
	JustCreated bool      `json:"just_created"`
}

func (mx *WebsocketCommandHandler) handleWSStartDM(cmd appservice.WebsocketCommand) (bool, interface{}) {
	var req StartDMRequest
	if err := json.Unmarshal(cmd.Data, &req); err != nil {
		return false, fmt.Errorf("failed to parse request: %w", err)
	}
	req.ActuallyStart = cmd.Command == "start_dm"
	resp, err := mx.StartChat(req)
	if err != nil {
		mx.log.Errorfln("Error in %s handler", cmd.Command)
		return false, err
	} else {
		return true, resp
	}
}

func (mx *WebsocketCommandHandler) handleWSCreateGroup(cmd appservice.WebsocketCommand) (bool, interface{}) {
	var req CreateGroupRequest
	if err := json.Unmarshal(cmd.Data, &req); err != nil {
		return false, fmt.Errorf("failed to parse request: %w", err)
	}
	resp, err := mx.CreateGroup(req)
	if err != nil {
		return false, err
	}
	return true, resp
}

func copyStartDMResponse(resp *StartDMResponse) *StartDMResponse {
	if resp == nil {
		return nil
	}
	copy := *resp
	return &copy
}

func (mx *WebsocketCommandHandler) CreateGroup(req CreateGroupRequest) (*StartDMResponse, error) {
	if req.Message == "" {
		return mx.createGroup(req)
	} else if strings.TrimSpace(req.TransactionID) == "" {
		return nil, fmt.Errorf("a stable transaction_id is required when sending a first message")
	}
	req.Users = append([]string(nil), req.Users...)
	payload, err := json.Marshal(req)
	if err != nil {
		return nil, fmt.Errorf("failed to encode group request: %w", err)
	}
	digest := sha256.Sum256(payload)
	mx.groupCreateLock.Lock()
	now := time.Now()
	for txnID, txn := range mx.groupCreates {
		if !txn.expiresAt.IsZero() && !now.Before(txn.expiresAt) {
			delete(mx.groupCreates, txnID)
		}
	}
	if txn, ok := mx.groupCreates[req.TransactionID]; ok {
		mx.groupCreateLock.Unlock()
		if txn.digest != digest {
			return nil, fmt.Errorf("transaction_id was already used with a different group request")
		}
		<-txn.done
		return copyStartDMResponse(txn.response), txn.err
	}
	if len(mx.groupCreates) >= 256 {
		mx.groupCreateLock.Unlock()
		return nil, fmt.Errorf("group transaction cache is full; no message was sent for this new transaction")
	}
	if mx.groupCreates == nil {
		mx.groupCreates = make(map[string]*groupCreateTransaction)
	}
	// shortcut: deduplication does not survive restart; clients must not blindly replay uncertain sends after restart.
	txn := &groupCreateTransaction{digest: digest, done: make(chan struct{})}
	mx.groupCreates[req.TransactionID] = txn
	mx.groupCreateLock.Unlock()

	resp, err := mx.createGroup(req)
	mx.groupCreateLock.Lock()
	txn.response, txn.err = copyStartDMResponse(resp), err
	txn.expiresAt = time.Now().Add(24 * time.Hour)
	close(txn.done)
	mx.groupCreateLock.Unlock()
	return resp, err
}

func validChatGUID(guid string, group bool) bool {
	parts := strings.Split(guid, ";")
	if len(parts) != 3 || strings.TrimSpace(parts[2]) == "" || strings.IndexFunc(parts[2], unicode.IsControl) >= 0 {
		return false
	}
	if parts[0] != "iMessage" && parts[0] != "SMS" && parts[0] != "RCS" && parts[0] != "any" {
		return false
	}
	return (group && parts[1] == "+") || (!group && parts[1] == "-")
}

func uncertainGroupError(err error) error {
	return fmt.Errorf("%w; the group and first message may already exist: do not repeat blindly", err)
}

func (mx *WebsocketCommandHandler) createGroup(req CreateGroupRequest) (*StartDMResponse, error) {
	messageAPI, withMessage := mx.bridge.IM.(imessage.GroupChatWithMessageAPI)
	if withMessage && strings.TrimSpace(req.Message) == "" {
		return nil, fmt.Errorf("this connector requires a nonempty first message; use create-group <recipient> <recipient> -- <first message>")
	} else if !withMessage && req.Message != "" {
		return nil, fmt.Errorf("this connector does not support creating a group with a first message; no message was sent")
	}
	if withMessage && (len(req.Users) < 2 || len(req.Users) > 31) {
		return nil, fmt.Errorf("a group requires 2–31 distinct recipients")
	}
	guids := make([]string, len(req.Users))
	seen := make(map[string]bool)
	var hasSMS, hasEmail bool
	for i, identifier := range req.Users {
		var err error
		if !withMessage && (strings.HasPrefix(identifier, "iMessage;-;") || strings.HasPrefix(identifier, "SMS;-;")) {
			guids[i] = identifier
		} else {
			guids[i], err = mx.bridge.IM.ResolveIdentifier(identifier)
			if err != nil {
				return nil, fmt.Errorf("failed to resolve group recipient %d: %w", i+1, err)
			}
		}
		if withMessage {
			if !validChatGUID(guids[i], false) {
				return nil, fmt.Errorf("group recipient %d did not resolve to a valid direct identifier", i+1)
			}
			parsed := imessage.ParseIdentifier(guids[i])
			if parsed.Service == "RCS" || strings.HasPrefix(identifier, "RCS;") {
				return nil, fmt.Errorf("RCS group creation is not supported")
			} else if parsed.Service != "iMessage" && parsed.Service != "SMS" {
				return nil, fmt.Errorf("group recipient %d resolved to an unsupported service", i+1)
			}
			key := strings.ToLower(parsed.LocalID)
			if seen[key] {
				return nil, fmt.Errorf("group recipients must be distinct after normalization")
			}
			seen[key] = true
			hasSMS = hasSMS || parsed.Service == "SMS"
			hasEmail = hasEmail || !isNumber(parsed.LocalID)
		}
		if strings.HasPrefix(guids[i], "SMS;-;") && !req.AllowSMS {
			return nil, fmt.Errorf("group recipient %d is only available on SMS; explicit allow_sms consent is required", i+1)
		}
	}
	if withMessage && hasSMS && hasEmail {
		return nil, fmt.Errorf("SMS groups require phone numbers for all recipients; email addresses cannot be downgraded")
	}
	var createResp *imessage.CreateGroupResponse
	var err error
	if withMessage {
		createResp, err = messageAPI.CreateGroupWithMessage(guids, req.Message)
	} else {
		createResp, err = mx.bridge.IM.CreateGroup(guids)
	}
	if err != nil {
		err = fmt.Errorf("failed to create group: %w", err)
		if withMessage {
			err = uncertainGroupError(err)
		}
		return nil, err
	}
	if createResp == nil || !validChatGUID(createResp.GUID, true) {
		err = fmt.Errorf("connector returned an invalid group identifier")
	} else if withMessage && ((hasSMS && !strings.HasPrefix(createResp.GUID, "SMS;+;")) || (!hasSMS && !strings.HasPrefix(createResp.GUID, "iMessage;+;"))) {
		err = fmt.Errorf("connector returned an unexpected group service")
	}
	if err != nil {
		if withMessage {
			err = uncertainGroupError(err)
		}
		return nil, err
	}
	portal := mx.bridge.GetPortalByGUID(createResp.GUID)
	portal.roomCreateLock.Lock()
	if !withMessage {
		portal.ThreadID = createResp.ThreadID
	} else if portal.ThreadID == "" {
		err = portal.updateThreadID(createResp.ThreadID)
	}
	resp := StartDMResponse{
		RoomID:      portal.MXID,
		GUID:        createResp.GUID,
		JustCreated: len(portal.MXID) == 0,
	}
	portal.roomCreateLock.Unlock()
	if err != nil {
		return nil, uncertainGroupError(fmt.Errorf("failed to persist conversation identity: %w", err))
	}
	err = portal.CreateMatrixRoom(nil, nil)
	if err != nil {
		err = fmt.Errorf("failed to create Matrix room: %w", err)
		if withMessage {
			err = uncertainGroupError(err)
		}
		return nil, err
	}
	resp.RoomID = portal.roomID()
	return &resp, nil
}

func (mx *WebsocketCommandHandler) handleWSGetContacts(_ appservice.WebsocketCommand) (bool, interface{}) {
	contacts, err := mx.bridge.IM.GetContactList()
	if err != nil {
		return false, err
	}
	return true, contacts
}

type UploadContactsRequest struct {
	Contacts []*imessage.Contact `json:"contacts"`
}

func (mx *WebsocketCommandHandler) handleWSUploadContacts(cmd appservice.WebsocketCommand) (bool, any) {
	var req UploadContactsRequest
	if err := json.Unmarshal(cmd.Data, &req); err != nil {
		return false, fmt.Errorf("failed to parse request: %w", err)
	}
	mx.bridge.UpdateMerges(req.Contacts)
	return true, nil
}

func isNumber(number string) bool {
	if number == "" || number == "+" {
		return false
	}
	for i, char := range number {
		if (char < '0' || char > '9') && !(i == 0 && char == '+') {
			return false
		}
	}
	return true
}

func (mx *WebsocketCommandHandler) trackResolveIdentifier(actuallyTrack bool, identifier, status string) {
	if actuallyTrack {
		identifierType := "unknown"
		isStartupTarget := identifier == mx.bridge.Config.HackyStartupTest.Identifier
		if validChatGUID(identifier, false) {
			identifier = imessage.ParseIdentifier(identifier).LocalID
		}
		phone := strings.NewReplacer(" ", "", "-", "", "(", "", ")", "").Replace(strings.TrimPrefix(identifier, "tel:"))
		if isNumber(phone) {
			identifierType = "phone"
		} else if strings.ContainsRune(identifier, '@') {
			identifierType = "email"
		}
		Segment.Track("iMC resolve identifier", map[string]any{
			"status":            status,
			"is_startup_target": strconv.FormatBool(isStartupTarget),
			"identifier_type":   identifierType,
		})
	}
}

func (mx *WebsocketCommandHandler) StartChat(req StartDMRequest) (*StartDMResponse, error) {
	var resp StartDMResponse
	var err error
	var forced bool

	if resp.GUID, err = mx.bridge.IM.ResolveIdentifier(req.Identifier); err != nil {
		if req.Force && req.ActuallyStart && !errors.Is(err, imessage.ErrIdentifierResolution) {
			mx.log.Debugln("Failed to resolve identifier, but forcing creation anyway")
			resp.GUID = req.Identifier
			forced = true
		} else {
			mx.trackResolveIdentifier(!req.ActuallyStart, req.Identifier, "fail")
			return nil, fmt.Errorf("failed to resolve identifier: %w", err)
		}
	}
	if !validChatGUID(resp.GUID, false) {
		mx.trackResolveIdentifier(!req.ActuallyStart, req.Identifier, "fail")
		return nil, fmt.Errorf("resolved identifier is not a valid direct chat GUID")
	}
	if parsed := imessage.ParseIdentifier(resp.GUID); parsed.Service == "SMS" && !isNumber(parsed.LocalID) {
		mx.trackResolveIdentifier(!req.ActuallyStart, req.Identifier, "fail")
		return nil, fmt.Errorf("can't start SMS with non-numeric identifier")
	} else if portal := mx.bridge.GetPortalByGUID(resp.GUID); len(portal.MXID) > 0 || !req.ActuallyStart {
		status := "success"
		if parsed.Service == "SMS" {
			status = "sms"
		}
		if !forced {
			mx.trackResolveIdentifier(!req.ActuallyStart, req.Identifier, status)
		}
		resp.RoomID = portal.MXID
		return &resp, nil
	} else if err = mx.bridge.IM.PrepareDM(resp.GUID); err != nil {
		return nil, fmt.Errorf("failed to prepare DM: %w", err)
	} else if err = portal.CreateMatrixRoom(nil, &req.ProfileOverride); err != nil {
		return nil, fmt.Errorf("failed to create Matrix room: %w", err)
	} else {
		resp.JustCreated = true
		resp.RoomID = portal.MXID
		return &resp, nil
	}
}

func (mx *WebsocketCommandHandler) HandleSyncProxyError(syncErr *mautrix.RespError, startErr error) {
	if !atomic.CompareAndSwapInt64(&mx.syncProxyWaiting, 0, 1) {
		var err interface{} = startErr
		if err == nil {
			err = syncErr.Err
		}
		mx.log.Debugfln("Got sync proxy error (%v), but there's already another thread waiting to restart sync proxy", err)
		return
	}
	if time.Now().Sub(mx.lastSyncProxyError) < MaxSyncProxyBackoff {
		mx.syncProxyBackoff *= 2
		if mx.syncProxyBackoff > MaxSyncProxyBackoff {
			mx.syncProxyBackoff = MaxSyncProxyBackoff
		}
	} else {
		mx.syncProxyBackoff = DefaultSyncProxyBackoff
	}
	mx.lastSyncProxyError = time.Now()
	if syncErr != nil {
		mx.log.Errorfln("Syncproxy told us that syncing failed: %s - Requesting a restart in %s", syncErr.Err, mx.syncProxyBackoff)
	} else if startErr != nil {
		mx.log.Errorfln("Failed to request sync proxy to start syncing: %v - Requesting a restart in %s", startErr, mx.syncProxyBackoff)
	}
	time.Sleep(mx.syncProxyBackoff)
	atomic.StoreInt64(&mx.syncProxyWaiting, 0)
	mx.bridge.RequestStartSync()
}

type NewRoomBackfillRequest struct {
	Chats []*imessage.ChatInfo `json:"chats"`
}

func (mx *WebsocketCommandHandler) handleCreateRoomsForBackfill(cmd appservice.WebsocketCommand) (bool, any) {
	var req NewRoomBackfillRequest
	if err := json.Unmarshal(cmd.Data, &req); err != nil {
		return false, fmt.Errorf("failed to parse request: %w", err)
	}

	mx.log.Debugfln("Got request to create %d rooms for backfill", len(req.Chats))
	go func() {
		mx.createRoomsForBackfillError = nil
		for _, info := range req.Chats {
			info.Identifier = imessage.ParseIdentifier(info.JSONChatGUID)
			portals := mx.bridge.FindPortalsByThreadID(info.ThreadID)
			var portal *Portal
			if len(portals) > 1 {
				mx.log.Warnfln("Found multiple portals with thread ID %s (message chat guid: %s)", info.ThreadID, info.Identifier.String())
				continue
			} else if len(portals) == 1 {
				portal = portals[0]
			} else {
				// This will create the new portal
				portal = mx.bridge.GetPortalByGUID(info.Identifier.String())
			}

			if len(portal.MXID) == 0 {
				portal.zlog.Info().Msg("Creating Matrix room with latest chat info")
				err := portal.CreateMatrixRoom(info, nil)
				if err != nil {
					mx.createRoomsForBackfillError = err
					return
				}
			} else {
				portal.zlog.Info().Msg("Syncing Matrix room with latest chat info")
				portal.SyncWithInfo(info)
			}

			mx.log.Debugfln("Room %s created for backfilling %s", portal.MXID, info.JSONChatGUID)
		}
	}()
	return true, struct{}{}
}

type RoomInfoForBackfillRequest struct {
	ChatGUIDs []string `json:"chats_guids"`
}

type RoomInfoForBackfill struct {
	RoomID                   id.RoomID `json:"room_id"`
	EarliestBridgedTimestamp int64     `json:"earliest_bridged_timestamp"`
}

type RoomCreationForBackfillStatus string

const (
	RoomCreationForBackfillStatusInProgress RoomCreationForBackfillStatus = "in-progress"
	RoomCreationForBackfillStatusDone       RoomCreationForBackfillStatus = "done"
	RoomCreationForBackfillStatusError      RoomCreationForBackfillStatus = "error"
)

type RoomInfoForBackfillResponse struct {
	Status RoomCreationForBackfillStatus  `json:"status"`
	Error  string                         `json:"error,omitempty"`
	Rooms  map[string]RoomInfoForBackfill `json:"rooms,omitempty"`
}

func (mx *WebsocketCommandHandler) handleGetRoomInfoForBackfill(cmd appservice.WebsocketCommand) (bool, any) {
	if mx.createRoomsForBackfillError != nil {
		return true, RoomInfoForBackfillResponse{
			Status: RoomCreationForBackfillStatusError,
			Error:  mx.createRoomsForBackfillError.Error(),
		}
	}

	var req RoomInfoForBackfillRequest
	if err := json.Unmarshal(cmd.Data, &req); err != nil {
		return false, fmt.Errorf("failed to parse request: %w", err)
	}

	resp := RoomInfoForBackfillResponse{
		Status: RoomCreationForBackfillStatusDone,
		Rooms:  map[string]RoomInfoForBackfill{},
	}
	now := time.Now().UnixMilli()
	mx.log.Debugfln("Got request to get room info for backfills")
	for _, chatGUID := range req.ChatGUIDs {
		portal := mx.bridge.GetPortalByGUID(chatGUID)

		if len(portal.MXID) == 0 {
			return true, RoomInfoForBackfillResponse{Status: RoomCreationForBackfillStatusInProgress}
		}

		timestamp, err := mx.bridge.DB.Message.GetEarliestTimestampInChat(chatGUID)
		if err != nil || timestamp < 0 {
			timestamp = now
		}
		resp.Rooms[portal.GUID] = RoomInfoForBackfill{
			RoomID:                   portal.MXID,
			EarliestBridgedTimestamp: timestamp,
		}
	}

	return true, resp
}
