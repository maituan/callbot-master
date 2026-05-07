package bot

import (
	"reflect"
	"testing"
)

// feedAll feeds text to the parser one chunk per slice element and returns
// every sentence emitted across all Feed calls (in order).
func feedAll(p *SentenceParser, chunks []string) []string {
	var got []string
	for _, c := range chunks {
		got = append(got, p.Feed(c)...)
	}
	return got
}

func TestParser_TwoSentencesWithAction(t *testing.T) {
	p := &SentenceParser{}
	got := feedAll(p, []string{"Xin chào. Anh chị cần giúp gì ạ?|CHAT"})
	want := []string{"Xin chào.", "Anh chị cần giúp gì ạ?"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("sentences = %#v, want %#v", got, want)
	}
	leftover, action := p.Finalize()
	if leftover != "" {
		t.Fatalf("leftover = %q, want empty", leftover)
	}
	if action != ActionChat {
		t.Fatalf("action = %q, want CHAT", action)
	}
}

func TestParser_GreetingCharByChar(t *testing.T) {
	p := &SentenceParser{}
	greeting := "Xin chào, em là callbot ạ.|CHAT"
	var got []string
	for _, r := range greeting {
		got = append(got, p.Feed(string(r))...)
	}
	want := []string{"Xin chào, em là callbot ạ."}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("sentences = %#v, want %#v", got, want)
	}
	leftover, action := p.Finalize()
	if leftover != "" {
		t.Fatalf("leftover = %q, want empty", leftover)
	}
	if action != ActionChat {
		t.Fatalf("action = %q, want CHAT", action)
	}
}

func TestParser_PipeMidContentUsesLastIndex(t *testing.T) {
	// Rare but possible: text contains a `|`. LastIndex must still pick the
	// trailing action delimiter, not the embedded pipe.
	p := &SentenceParser{}
	got := feedAll(p, []string{"Chọn A | hoặc B. Cám ơn anh.|ENDCALL"})
	want := []string{"Chọn A | hoặc B.", "Cám ơn anh."}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("sentences = %#v, want %#v", got, want)
	}
	leftover, action := p.Finalize()
	if leftover != "" {
		t.Fatalf("leftover = %q, want empty", leftover)
	}
	if action != ActionEndCall {
		t.Fatalf("action = %q, want ENDCALL", action)
	}
}

func TestParser_EllipsisIsSentenceDelim(t *testing.T) {
	p := &SentenceParser{}
	got := feedAll(p, []string{"Để em xem… Dạ vâng.|CHAT"})
	want := []string{"Để em xem…", "Dạ vâng."}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("sentences = %#v, want %#v", got, want)
	}
	_, action := p.Finalize()
	if action != ActionChat {
		t.Fatalf("action = %q, want CHAT", action)
	}
}

func TestParser_NewlineAsDelim(t *testing.T) {
	p := &SentenceParser{}
	got := feedAll(p, []string{"Dòng 1\nDòng 2 dài hơn một chút.|CHAT"})
	want := []string{"Dòng 1", "Dòng 2 dài hơn một chút."}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("sentences = %#v, want %#v", got, want)
	}
}

func TestParser_NoActionDelim_FallbackChat(t *testing.T) {
	// Defensive: if upstream forgets the |ACTION suffix, default to CHAT
	// and emit any leftover content.
	p := &SentenceParser{}
	got := feedAll(p, []string{"Chỉ một câu thôi"})
	if len(got) != 0 {
		t.Fatalf("sentences = %#v, want none", got)
	}
	leftover, action := p.Finalize()
	if leftover != "Chỉ một câu thôi" {
		t.Fatalf("leftover = %q", leftover)
	}
	if action != ActionChat {
		t.Fatalf("action = %q, want CHAT", action)
	}
}

func TestParser_UnknownActionDefaultsToChat(t *testing.T) {
	p := &SentenceParser{}
	feedAll(p, []string{"Xin chào.|FOO"})
	_, action := p.Finalize()
	if action != ActionChat {
		t.Fatalf("action = %q, want CHAT (fallback)", action)
	}
}

func TestParser_ChunkBoundaryInsideRune(t *testing.T) {
	// Vietnamese chars are multi-byte UTF-8; ensure the parser handles
	// chunk boundaries that fall inside a rune.
	full := "Để em xem.|CHAT"
	for split := 1; split < len(full); split++ {
		p := &SentenceParser{}
		_ = feedAll(p, []string{full[:split], full[split:]})
		leftover, action := p.Finalize()
		// The decoded text is invariant; just check action is CHAT and
		// leftover is empty (sentence "Để em xem." was emitted).
		if leftover != "" {
			t.Fatalf("split=%d leftover=%q want empty", split, leftover)
		}
		if action != ActionChat {
			t.Fatalf("split=%d action=%q want CHAT", split, action)
		}
	}
}

func TestParser_OnlyActionNoContent(t *testing.T) {
	// Edge: empty content + action — shouldn't emit a sentence.
	p := &SentenceParser{}
	got := feedAll(p, []string{"|CHAT"})
	if len(got) != 0 {
		t.Fatalf("sentences = %#v, want none", got)
	}
	leftover, action := p.Finalize()
	if leftover != "" {
		t.Fatalf("leftover = %q, want empty", leftover)
	}
	if action != ActionChat {
		t.Fatalf("action = %q, want CHAT", action)
	}
}

func TestParser_TrailingFragmentEmittedAsLeftover(t *testing.T) {
	// Last sentence has no terminal delim before |ACTION (rare but possible).
	p := &SentenceParser{}
	got := feedAll(p, []string{"Câu một. Câu hai chưa kết|ENDCALL"})
	want := []string{"Câu một."}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("sentences = %#v, want %#v", got, want)
	}
	leftover, action := p.Finalize()
	if leftover != "Câu hai chưa kết" {
		t.Fatalf("leftover = %q", leftover)
	}
	if action != ActionEndCall {
		t.Fatalf("action = %q, want ENDCALL", action)
	}
}
