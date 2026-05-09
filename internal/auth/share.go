package auth

import (
	"errors"
	"fmt"
	"time"

	"github.com/golang-jwt/jwt/v5"
)

// shareIssuer is the JWT iss field for share tokens. Lets us tell apart
// a session JWT from a share token even though both are HS256-signed
// with the same secret.
const shareIssuer = "callbot-share"

// ShareClaims is what we encode into a share token.
type ShareClaims struct {
	CallID string `json:"sub"`
	jwt.RegisteredClaims
}

// IssueShareToken mints a JWT that grants read access to one specific
// call_id for the configured TTL. Stateless — no DB row, no revocation.
func (i *Issuer) IssueShareToken(callID string, ttl time.Duration) (string, time.Time, error) {
	if callID == "" {
		return "", time.Time{}, errors.New("call_id required")
	}
	if ttl <= 0 {
		ttl = 7 * 24 * time.Hour
	}
	exp := time.Now().Add(ttl)
	claims := ShareClaims{
		CallID: callID,
		RegisteredClaims: jwt.RegisteredClaims{
			Issuer:    shareIssuer,
			IssuedAt:  jwt.NewNumericDate(time.Now()),
			ExpiresAt: jwt.NewNumericDate(exp),
		},
	}
	tok := jwt.NewWithClaims(jwt.SigningMethodHS256, claims)
	signed, err := tok.SignedString(i.secret)
	if err != nil {
		return "", time.Time{}, fmt.Errorf("sign share token: %w", err)
	}
	return signed, exp, nil
}

// ParseShareToken verifies signature + iss + expiry and returns the
// call_id that the token authorises read access to.
func (i *Issuer) ParseShareToken(raw string) (string, error) {
	tok, err := jwt.ParseWithClaims(raw, &ShareClaims{}, func(t *jwt.Token) (any, error) {
		if _, ok := t.Method.(*jwt.SigningMethodHMAC); !ok {
			return nil, fmt.Errorf("unexpected alg: %v", t.Header["alg"])
		}
		return i.secret, nil
	})
	if err != nil {
		return "", err
	}
	c, ok := tok.Claims.(*ShareClaims)
	if !ok || !tok.Valid {
		return "", errors.New("invalid share token")
	}
	if c.Issuer != shareIssuer {
		return "", errors.New("token is not a share token")
	}
	return c.CallID, nil
}
