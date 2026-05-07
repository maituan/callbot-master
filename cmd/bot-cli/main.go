// Command bot-cli is a smoke-test client for the callbot-hcc-base-ts
// streaming endpoint. It pipes one user message through the bot and prints
// each flushed sentence + the final action.
//
// Example:
//
//	go run ./cmd/bot-cli -conv test-001 "tôi muốn cấp đổi căn cước"
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"callbot-master/internal/bot"
)

func main() {
	url := flag.String("url", "http://localhost:11006/api/v1/call/", "bot streaming endpoint")
	convID := flag.String("conv", "bot-cli-001", "conversation_id (= FS call uuid in production)")
	firstByte := flag.Duration("first-byte", 5*time.Second, "first-byte timeout")
	total := flag.Duration("total", 25*time.Second, "total turn timeout")
	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "usage: %s [flags] <message...>\n", os.Args[0])
		flag.PrintDefaults()
	}
	flag.Parse()

	args := flag.Args()
	if len(args) == 0 {
		flag.Usage()
		os.Exit(2)
	}
	msg := strings.Join(args, " ")

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	client := bot.NewHTTPClient(*url, *firstByte, *total)
	start := time.Now()
	ts, err := client.Stream(ctx, *convID, msg)
	if err != nil {
		fmt.Fprintf(os.Stderr, "stream error: %v\n", err)
		os.Exit(1)
	}
	defer ts.Close()

	firstSentenceAt := time.Duration(0)
	count := 0
	for s := range ts.Sentences() {
		count++
		if firstSentenceAt == 0 {
			firstSentenceAt = time.Since(start)
		}
		fmt.Printf("[sentence %d @ %dms] %s\n", count, time.Since(start).Milliseconds(), s)
	}
	action, err := ts.Action()
	total2 := time.Since(start)
	if err != nil {
		fmt.Fprintf(os.Stderr, "action error: %v\n", err)
		os.Exit(1)
	}
	fmt.Printf("[action] %s\n", action)
	fmt.Printf("[stats] sentences=%d first=%dms total=%dms\n",
		count, firstSentenceAt.Milliseconds(), total2.Milliseconds())
}
