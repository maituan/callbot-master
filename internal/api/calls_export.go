package api

import (
	"encoding/csv"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	"callbot-master/internal/auth"
	"callbot-master/internal/store"
)

// exportCalls handles GET /api/v1/calls/export — same filter shape as
// the list endpoint but returns a CSV the ops team can paste into Excel.
//
// Columns: STT, ID, Thể loại, Conversation, Audio, Thời lượng.
// `Conversation` is one cell with alternating "[user] …" / "[assistant] …"
// lines per turn so Excel renders it as a wrapped cell.
//
// Default window cap is 5000 rows (the store layer hard limit). If ops
// needs more, they paginate by tightening date range.
func (h *callsHandler) exportCSV(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeJSONError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	q := r.URL.Query()
	f := store.ListFilter{
		Scenario:  q.Get("scenario"),
		Direction: q.Get("direction"),
		QCStatus:  q.Get("qc_status"),
		Limit:     5000,
	}
	// Multi-phone: accept both `?phone=a&phone=b` and `?phones=a,b,c`.
	if vs := q["phone"]; len(vs) > 1 {
		f.Phones = vs
	} else if v := q.Get("phones"); v != "" {
		for _, p := range strings.Split(v, ",") {
			p = strings.TrimSpace(p)
			if p != "" {
				f.Phones = append(f.Phones, p)
			}
		}
	} else {
		f.Phone = q.Get("phone")
	}
	if v := q.Get("since"); v != "" {
		t, err := parseFlexTime(v)
		if err != nil {
			writeJSONError(w, http.StatusBadRequest, "since must be RFC3339")
			return
		}
		f.Since = t
	}
	if v := q.Get("until"); v != "" {
		t, err := parseFlexTime(v)
		if err != nil {
			writeJSONError(w, http.StatusBadRequest, "until must be RFC3339")
			return
		}
		f.Until = t
	}
	// Tenant scope from identity (same rule as the list endpoint).
	if id := auth.FromContext(r.Context()); id != nil {
		if !id.IsPlatformAdmin() {
			f.TenantID = id.TenantID
		}
	}

	records, err := h.d.Store.List(r.Context(), f)
	if err != nil {
		writeJSONError(w, http.StatusInternalServerError, "list calls: "+err.Error())
		return
	}

	filename := fmt.Sprintf("callbot-calls-%s.csv", time.Now().Format("20060102-150405"))
	w.Header().Set("Content-Type", "text/csv; charset=utf-8")
	w.Header().Set("Content-Disposition", `attachment; filename="`+filename+`"`)
	w.Header().Set("Cache-Control", "no-store")
	// Excel detects UTF-8 only with a BOM at the start of the file —
	// without it Vietnamese diacritics render as mojibake.
	_, _ = w.Write([]byte{0xEF, 0xBB, 0xBF})

	cw := csv.NewWriter(w)
	defer cw.Flush()

	_ = cw.Write([]string{
		"STT", "ID", "Thể loại", "SĐT", "Bắt đầu",
		"Conversation", "Link audio", "Thời lượng (s)", "Trạng thái", "Action", "QC",
	})

	for i, c := range records {
		qc := ""
		switch c.QCVerdict {
		case "like":
			qc = "Đạt"
		case "dislike":
			qc = "Không đạt"
		}
		_ = cw.Write([]string{
			strconv.Itoa(i + 1),
			c.CallID,
			c.Direction,
			c.Phone,
			c.StartTime.Format(time.RFC3339),
			conversationCell(c.History),
			c.RecordingURL,
			strconv.Itoa(c.DurationSec),
			c.Status,
			c.Action,
			qc,
		})
	}
}

// conversationCell turns a CallRecord.History slice into a single CSV
// cell with one [user]/[assistant] block per turn. Excel renders the
// embedded newlines correctly when wrap-text is on.
func conversationCell(turns []store.Turn) string {
	if len(turns) == 0 {
		return ""
	}
	var b strings.Builder
	for _, t := range turns {
		if t.UserText != "" {
			b.WriteString("[user] ")
			b.WriteString(t.UserText)
			b.WriteString("\n")
		}
		if t.BotText != "" {
			b.WriteString("[assistant] ")
			b.WriteString(t.BotText)
			b.WriteString("\n")
		}
	}
	return strings.TrimRight(b.String(), "\n")
}
