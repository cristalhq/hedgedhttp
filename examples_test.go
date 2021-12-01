package hedgedhttp_test

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"net/http"
	"time"

	"github.com/cristalhq/hedgedhttp"
)

func ExampleHedged() {
	ctx := context.Background()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, "https://google.com", http.NoBody)
	if err != nil {
		panic(err)
	}

	timeout := 10 * time.Millisecond
	upto := 7
	transport := http.DefaultTransport
	hedged, stats := hedgedhttp.NewRoundTripperAndStats(timeout, upto, transport)

	// print stats periodically
	go func() {
		for {
			time.Sleep(time.Second)
			fmt.Printf("stats: %+v\n", stats)
		}
	}()

	// will take `upto` requests, with a `timeout` delay between them
	resp, err := hedged.RoundTrip(req)
	if err != nil {
		panic(err)
	}
	defer resp.Body.Close()

	// and do something with resp
}

func ExampleInstrumented() {
	transport := &InstrumentedTransport{
		Transport: http.DefaultTransport,
	}

	timeout := 10 * time.Millisecond
	upto := 7

	_ = hedgedhttp.NewRoundTripper(timeout, upto, transport)
}

type InstrumentedTransport struct {
	Transport http.RoundTripper // Used to make actual requests.
}

func (t *InstrumentedTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	// NOTE: log, update metric, add trace to the req variable.

	resp, err := t.Transport.RoundTrip(req)
	if err != nil {
		return nil, err
	}

	// NOTE: log, update metric based on the resp variable.

	return resp, nil
}

func ExampleRatelimit() {
	transport := &RateLimitedHedgedTransport{
		Transport: http.DefaultTransport,
		Limiter:   &RandomRateLimiter{},
	}

	timeout := 10 * time.Millisecond
	upto := 7

	_ = hedgedhttp.NewRoundTripper(timeout, upto, transport)
}

// by example https://pkg.go.dev/golang.org/x/time/rate
type RateLimiter interface {
	Wait(ctx context.Context) error
}

type RateLimitedHedgedTransport struct {
	Transport http.RoundTripper // Used to make actual requests.
	Limiter   RateLimiter       // Any ratelimit-like thing.
}

func (t *RateLimitedHedgedTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	// apply rate limit only for hedged requests
	if hedgedhttp.IsHedgedRequest(r) {
		if err := t.Limiter.Wait(r.Context()); err != nil {
			return nil, err
		}
	}
	return t.Transport.RoundTrip(r)
}

// Just for the example
type RandomRateLimiter struct{}

func (r *RandomRateLimiter) Wait(ctx context.Context) error {
	if rand.Int()%2 == 0 {
		return errors.New("rate limit exceed")
	}
	return nil
}
