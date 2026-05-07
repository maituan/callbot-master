// Package auth handles login: password hashing, JWT issue/verify,
// and the HTTP middleware that injects identity into request context.
package auth

import "golang.org/x/crypto/bcrypt"

// bcryptCost balances login latency vs. brute-force resistance. 12 ≈ 250ms
// on a typical server core; raise to 13 if you have headroom.
const bcryptCost = 12

func HashPassword(plain string) (string, error) {
	b, err := bcrypt.GenerateFromPassword([]byte(plain), bcryptCost)
	if err != nil {
		return "", err
	}
	return string(b), nil
}

// VerifyPassword returns true on match. Returns false (no error) for
// either a bad password or a malformed hash — callers should treat both
// as "wrong credentials" to avoid leaking whether the user exists.
func VerifyPassword(hash, plain string) bool {
	return bcrypt.CompareHashAndPassword([]byte(hash), []byte(plain)) == nil
}
