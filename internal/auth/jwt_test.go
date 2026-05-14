package auth

import (
	"testing"
	"time"

	"github.com/google/uuid"
)

func TestIssueAndParse(t *testing.T) {
	const secret = "this-is-a-fake-test-secret-1234567890"
	iss, err := NewIssuer(secret, time.Hour)
	if err != nil {
		t.Fatalf("NewIssuer: %v", err)
	}
	uid := uuid.New()
	tid := uuid.New()
	tok, exp, err := iss.Issue(uid, "alice", "tenant_user", &tid, true)
	if err != nil {
		t.Fatalf("Issue: %v", err)
	}
	if tok == "" {
		t.Fatal("empty token")
	}
	if !exp.After(time.Now()) {
		t.Fatalf("exp not in the future: %v", exp)
	}
	c, err := iss.Parse(tok)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	if c.UserID != uid || c.Username != "alice" || c.Role != "tenant_user" {
		t.Fatalf("claims mismatch: %+v", c)
	}
	if c.TenantID == nil || *c.TenantID != tid {
		t.Fatalf("tenant id missing: %+v", c.TenantID)
	}
	if !c.IsEvaluator {
		t.Fatal("is_evaluator should round-trip true")
	}
}

func TestParseRejectsTamper(t *testing.T) {
	iss, _ := NewIssuer("this-is-a-fake-test-secret-1234567890", time.Hour)
	tok, _, _ := iss.Issue(uuid.New(), "x", "platform_admin", nil, false)
	// Mutate the signature segment by appending a char so the base64-decoded
	// signature no longer matches.
	tampered := tok + "x"
	if _, err := iss.Parse(tampered); err == nil {
		t.Fatal("expected error on tampered token")
	}
}

func TestNewIssuerRejectsShortSecret(t *testing.T) {
	if _, err := NewIssuer("short", time.Hour); err == nil {
		t.Fatal("expected short-secret error")
	}
}

func TestPasswordRoundtrip(t *testing.T) {
	h, err := HashPassword("hunter2")
	if err != nil {
		t.Fatalf("hash: %v", err)
	}
	if !VerifyPassword(h, "hunter2") {
		t.Fatal("verify failed for correct password")
	}
	if VerifyPassword(h, "wrong") {
		t.Fatal("verify accepted wrong password")
	}
	if VerifyPassword("not-a-hash", "hunter2") {
		t.Fatal("verify accepted malformed hash")
	}
}
