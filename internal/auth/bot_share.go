package auth

import (
	"errors"
	"fmt"
	"time"

	"github.com/golang-jwt/jwt/v5"
)

// botShareIssuer separates bot-share JWTs from call-share JWTs even though
// both are signed with the same HS256 secret. Possession of a bot-share
// token grants the holder access to /api/v1/web/* (chat + voice) for ONE
// specific bot until the token expires.
const botShareIssuer = "callbot-bot-share"

// BotShareChannel limits which surfaces a token unlocks. "both" lets the
// landing page show chat + voice buttons; channel-specific tokens hide
// the other.
type BotShareChannel string

const (
	BotShareChannelChat  BotShareChannel = "chat"
	BotShareChannelVoice BotShareChannel = "voice"
	BotShareChannelBoth  BotShareChannel = "both"
)

// BotShareClaims carries the bot id + which channels are unlocked.
type BotShareClaims struct {
	BotID   string `json:"sub"`
	Channel string `json:"chn,omitempty"` // "chat" | "voice" | "both"
	jwt.RegisteredClaims
}

// IssueBotShareToken mints a JWT that lets an unauthenticated visitor
// open the chat/voice playground for one bot. Stateless — no DB row, no
// revocation. TTL caps at 30d at the call site.
func (i *Issuer) IssueBotShareToken(botID string, channel BotShareChannel, ttl time.Duration) (string, time.Time, error) {
	if botID == "" {
		return "", time.Time{}, errors.New("bot_id required")
	}
	switch channel {
	case "":
		channel = BotShareChannelBoth
	case BotShareChannelChat, BotShareChannelVoice, BotShareChannelBoth:
	default:
		return "", time.Time{}, fmt.Errorf("invalid channel: %s", channel)
	}
	if ttl <= 0 {
		ttl = 7 * 24 * time.Hour
	}
	now := time.Now()
	exp := now.Add(ttl)
	claims := BotShareClaims{
		BotID:   botID,
		Channel: string(channel),
		RegisteredClaims: jwt.RegisteredClaims{
			Issuer:    botShareIssuer,
			IssuedAt:  jwt.NewNumericDate(now),
			ExpiresAt: jwt.NewNumericDate(exp),
		},
	}
	tok := jwt.NewWithClaims(jwt.SigningMethodHS256, claims)
	signed, err := tok.SignedString(i.secret)
	if err != nil {
		return "", time.Time{}, fmt.Errorf("sign bot-share token: %w", err)
	}
	return signed, exp, nil
}

// ParseBotShareToken verifies signature + iss + expiry and returns the
// bot id + channel grant. Caller compares the requested action (chat or
// voice) against the channel grant.
func (i *Issuer) ParseBotShareToken(raw string) (botID string, channel BotShareChannel, iat time.Time, err error) {
	tok, err := jwt.ParseWithClaims(raw, &BotShareClaims{}, func(t *jwt.Token) (any, error) {
		if _, ok := t.Method.(*jwt.SigningMethodHMAC); !ok {
			return nil, fmt.Errorf("unexpected alg: %v", t.Header["alg"])
		}
		return i.secret, nil
	})
	if err != nil {
		return "", "", time.Time{}, err
	}
	c, ok := tok.Claims.(*BotShareClaims)
	if !ok || !tok.Valid {
		return "", "", time.Time{}, errors.New("invalid bot-share token")
	}
	if c.Issuer != botShareIssuer {
		return "", "", time.Time{}, errors.New("token is not a bot-share token")
	}
	ch := BotShareChannel(c.Channel)
	if ch == "" {
		ch = BotShareChannelBoth
	}
	if c.IssuedAt != nil {
		iat = c.IssuedAt.Time
	}
	return c.BotID, ch, iat, nil
}

// ChannelAllows reports whether a token grant covers the requested channel.
func ChannelAllows(grant BotShareChannel, want BotShareChannel) bool {
	if grant == BotShareChannelBoth {
		return true
	}
	return grant == want
}
