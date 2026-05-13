package bot

import (
	"strings"
	"unicode/utf8"
)

// Sentence delimiters: full stop, question, exclamation, ellipsis, newline.
// We match the rule from CLAUDE.md "Bot API contract".
func isSentenceDelim(r rune) bool {
	switch r {
	case '.', '?', '!', '\n', '…':
		return true
	}
	return false
}

// SentenceParser converts a streaming UTF-8 text feed into discrete sentences
// and (on Finalize) the trailing "|<ACTION>" suffix.
//
// Rules (per CLAUDE.md):
//   - Flush a sentence as soon as a delimiter rune is seen.
//   - Never split on `|` mid-stream (Vietnamese text may contain it).
//   - Action is the substring after the LAST `|` in the full received text.
//
// Not safe for concurrent use; the HTTP stream owns one parser per turn.
type SentenceParser struct {
	full      strings.Builder
	flushedTo int // byte offset in full.String() up to which we already emitted
}

// Feed appends chunk and returns sentences completed by this chunk
// (in order). Empty/whitespace-only sentences are skipped.
func (p *SentenceParser) Feed(chunk string) []string {
	if chunk == "" {
		return nil
	}
	p.full.WriteString(chunk)
	s := p.full.String()
	var out []string
	for i := p.flushedTo; i < len(s); {
		r, size := utf8.DecodeRuneInString(s[i:])
		i += size
		if isSentenceDelim(r) {
			seg := strings.TrimSpace(s[p.flushedTo:i])
			if seg != "" {
				out = append(out, seg)
			}
			p.flushedTo = i
		}
	}
	return out
}

// Finalize is called once the upstream connection closes. It returns:
//   - leftover: any text that wasn't yet flushed (e.g. final fragment with no
//     trailing delimiter), excluding the "|<ACTION>" suffix. Empty if nothing left.
//   - action: ActionChat | ActionEndCall (or ActionChat as fallback).
func (p *SentenceParser) Finalize() (leftover string, action Action) {
	full := p.full.String()
	contentEnd := len(full)
	action = ActionChat
	if i := strings.LastIndex(full, "|"); i >= 0 {
		contentEnd = i
		// Prefix-match the action token so junk appended by an HTTP
		// proxy (e.g. nginx 1.18 inlining `Content-Length: 0` after
		// the last chunk) doesn't break ENDCALL detection. The bot
		// only ever emits CHAT or ENDCALL as the very first word
		// after the `|`, so anything trailing is noise we can drop.
		raw := strings.TrimSpace(full[i+1:])
		upper := strings.ToUpper(raw)
		switch {
		case strings.HasPrefix(upper, string(ActionEndCall)):
			action = ActionEndCall
		case strings.HasPrefix(upper, string(ActionChat)):
			action = ActionChat
		default:
			// Unknown / empty → default to CHAT, log responsibility falls
			// to the caller (HTTP stream wraps this).
			action = ActionChat
		}
	}
	if p.flushedTo < contentEnd {
		seg := strings.TrimSpace(full[p.flushedTo:contentEnd])
		if seg != "" {
			leftover = seg
		}
	}
	return
}
