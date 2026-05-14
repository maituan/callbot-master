package auth

import (
	"errors"
	"fmt"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"github.com/google/uuid"
)

// Claims is what we encode into the JWT. We keep it minimal — every byte
// is shipped on every request.
type Claims struct {
	UserID   uuid.UUID  `json:"sub"`
	Username string     `json:"usr"`
	Role     string     `json:"rol"`
	TenantID *uuid.UUID `json:"tid,omitempty"`
	// IsEvaluator opts a tenant_user into QC submissions. omitempty so
	// pre-feature tokens (no field set) parse as false without complaints.
	IsEvaluator bool `json:"qce,omitempty"`
	jwt.RegisteredClaims
}

// Issuer signs new tokens. Holds the symmetric secret so callers don't
// have to thread it through the call stack.
type Issuer struct {
	secret []byte
	ttl    time.Duration
}

func NewIssuer(secret string, ttl time.Duration) (*Issuer, error) {
	if len(secret) < 32 {
		return nil, errors.New("jwt secret must be at least 32 chars")
	}
	if ttl <= 0 {
		ttl = 12 * time.Hour
	}
	return &Issuer{secret: []byte(secret), ttl: ttl}, nil
}

// Issue mints a token for the given identity. Expiry is now + i.ttl.
func (i *Issuer) Issue(userID uuid.UUID, username, role string, tenantID *uuid.UUID, isEvaluator bool) (string, time.Time, error) {
	exp := time.Now().Add(i.ttl)
	claims := Claims{
		UserID:      userID,
		Username:    username,
		Role:        role,
		TenantID:    tenantID,
		IsEvaluator: isEvaluator,
		RegisteredClaims: jwt.RegisteredClaims{
			Issuer:    "callbot-master",
			IssuedAt:  jwt.NewNumericDate(time.Now()),
			ExpiresAt: jwt.NewNumericDate(exp),
		},
	}
	tok := jwt.NewWithClaims(jwt.SigningMethodHS256, claims)
	signed, err := tok.SignedString(i.secret)
	if err != nil {
		return "", time.Time{}, fmt.Errorf("sign jwt: %w", err)
	}
	return signed, exp, nil
}

// Parse verifies signature + expiry and returns the claims.
// Returns a clean error for any failure mode — don't leak internals.
func (i *Issuer) Parse(raw string) (*Claims, error) {
	tok, err := jwt.ParseWithClaims(raw, &Claims{}, func(t *jwt.Token) (any, error) {
		if _, ok := t.Method.(*jwt.SigningMethodHMAC); !ok {
			return nil, fmt.Errorf("unexpected alg: %v", t.Header["alg"])
		}
		return i.secret, nil
	})
	if err != nil {
		return nil, err
	}
	c, ok := tok.Claims.(*Claims)
	if !ok || !tok.Valid {
		return nil, errors.New("invalid token")
	}
	return c, nil
}

// TTL exposes the configured token lifetime so handlers can mirror it
// in the cookie's Max-Age.
func (i *Issuer) TTL() time.Duration { return i.ttl }
