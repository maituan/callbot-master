package api

import (
	"encoding/csv"
	"fmt"
	"net/http"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"

	"callbot-master/internal/auth"
	"callbot-master/internal/store"
)

// exportSessionsCSV handles GET /api/v1/web/sessions/export.
//
// Same filter shape as the list endpoint (bot_id, channel, status, range)
// plus tenant scope from identity. Each row carries:
//
//	STT, ID, Thể loại (chat|voice), Bắt đầu, Conversation,
//	Link audio (thư mục QC nếu có), Thời lượng (s), Trạng thái.
//
// Conversation is materialised on demand by calling GetWebSession for
// every row in the list — N+1, but bounded by the 500-row default cap
// and only triggered on explicit export action.
func (h *webHandler) exportSessionsCSV(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeJSONError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	id := auth.FromContext(r.Context())
	if id == nil {
		writeJSONError(w, http.StatusUnauthorized, "not authenticated")
		return
	}

	q := r.URL.Query()
	f := store.WebSessionFilter{
		Channel: q.Get("channel"),
		Status:  q.Get("status"),
		Limit:   500, // store hard-caps at 500
	}
	if v := q.Get("bot_id"); v != "" {
		bid, err := uuid.Parse(v)
		if err != nil {
			writeJSONError(w, http.StatusBadRequest, "invalid bot_id")
			return
		}
		f.BotID = bid
	}
	if !id.IsPlatformAdmin() && id.TenantID != nil {
		f.TenantID = *id.TenantID
	}

	// Range filter — applied client-side on started_at because the
	// list store doesn't carry one yet. Cheap on bounded 500-row sets.
	var since, until time.Time
	if v := q.Get("since"); v != "" {
		t, err := parseFlexTime(v)
		if err != nil {
			writeJSONError(w, http.StatusBadRequest, "since must be RFC3339")
			return
		}
		since = t
	}
	if v := q.Get("until"); v != "" {
		t, err := parseFlexTime(v)
		if err != nil {
			writeJSONError(w, http.StatusBadRequest, "until must be RFC3339")
			return
		}
		until = t
	}

	rows, err := h.d.Store.ListWebSessions(r.Context(), f)
	if err != nil {
		writeJSONError(w, http.StatusInternalServerError, "list sessions: "+err.Error())
		return
	}

	filename := fmt.Sprintf("callbot-web-sessions-%s.csv", time.Now().Format("20060102-150405"))
	w.Header().Set("Content-Type", "text/csv; charset=utf-8")
	w.Header().Set("Content-Disposition", `attachment; filename="`+filename+`"`)
	w.Header().Set("Cache-Control", "no-store")
	_, _ = w.Write([]byte{0xEF, 0xBB, 0xBF}) // UTF-8 BOM for Excel

	cw := csv.NewWriter(w)
	defer cw.Flush()

	_ = cw.Write([]string{
		"STT", "ID", "Thể loại", "Bắt đầu", "Kết thúc",
		"Conversation", "Link audio", "Thời lượng (s)", "Trạng thái", "IP",
	})

	stt := 0
	for _, s := range rows {
		// Range filter applied client-side (see comment above).
		if !since.IsZero() && s.StartedAt.Before(since) {
			continue
		}
		if !until.IsZero() && !s.StartedAt.Before(until) {
			continue
		}

		full, err := h.d.Store.GetWebSession(r.Context(), s.ID)
		if err != nil || full == nil {
			continue
		}

		stt++
		var duration int
		if full.EndedAt != nil {
			duration = int(full.EndedAt.Sub(full.StartedAt).Seconds())
		}
		audioLink := webSessionAudioLink(full)
		endedAt := ""
		if full.EndedAt != nil {
			endedAt = full.EndedAt.Format(time.RFC3339)
		}

		_ = cw.Write([]string{
			strconv.Itoa(stt),
			full.ID.String(),
			full.Channel,
			full.StartedAt.Format(time.RFC3339),
			endedAt,
			webConversationCell(full.Turns),
			audioLink,
			strconv.Itoa(duration),
			full.Status,
			full.IP,
		})
	}
}

// webConversationCell formats web_turn rows the same way as the call
// export — one [user]/[assistant] line per turn.
func webConversationCell(turns []*store.WebTurn) string {
	if len(turns) == 0 {
		return ""
	}
	var b strings.Builder
	for _, t := range turns {
		role := t.Role
		if role == "user" {
			b.WriteString("[user] ")
		} else {
			b.WriteString("[assistant] ")
		}
		b.WriteString(t.Text)
		b.WriteString("\n")
	}
	return strings.TrimRight(b.String(), "\n")
}

// webSessionAudioLink composes the public-relative URL to the per-turn
// WAV folder when recording was enabled. Returns "" for chat or for
// voice sessions started before MASTER_WEB_RECORDING_DIR was set.
func webSessionAudioLink(s *store.WebSession) string {
	if s.RecordingDir == "" || s.Channel != "voice" {
		return ""
	}
	// Stored RecordingDir is the absolute fs path; the public URL is
	// /web-recordings/<bot_id>/<session_id>/. Compute relative tail.
	return "/web-recordings/" + s.BotID.String() + "/" + s.ID.String() + "/"
}

// (used only for symmetry with calls_export.go — keep filepath import).
var _ = filepath.Join
