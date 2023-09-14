package hedgedhttp_test

import (
	"context"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"sync/atomic"
	"time"

	"github.com/cristalhq/hedgedhttp"
)

func ExampleClient() {
	ctx := context.Background()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, "https://google.com", http.NoBody)
	if err != nil {
		panic(err)
	}

	timeout := 10 * time.Millisecond
	upto := 7
	client := &http.Client{Timeout: time.Second}
	hedged, err := hedgedhttp.NewClient(timeout, upto, client)
	if err != nil {
		panic(err)
	}

	// will take `upto` requests, with a `timeout` delay between them
	resp, err := hedged.Do(req)
	if err != nil {
		panic(err)
	}
	defer resp.Body.Close()

	// and do something with resp

	// Output:
}

func Example_configNext() {
	rt := &observableRoundTripper{
		rt: http.DefaultTransport,
	}

	cfg := hedgedhttp.Config{
		Transport: rt,
		Upto:      3,
		Delay:     50 * time.Millisecond,
		Next: func() (upto int, delay time.Duration) {
			return 3, rt.MaxLatency()
		},
	}
	client, err := hedgedhttp.New(cfg)
	if err != nil {
		panic(err)
	}

	// or client.Do
	resp, err := client.RoundTrip(&http.Request{})
	_ = resp

	// Output:
}

func ExampleRoundTripper() {
	ctx := context.Background()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, "https://google.com", http.NoBody)
	if err != nil {
		panic(err)
	}

	timeout := 10 * time.Millisecond
	upto := 7
	transport := http.DefaultTransport
	hedged, stats, err := hedgedhttp.NewRoundTripperAndStats(timeout, upto, transport)
	if err != nil {
		panic(err)
	}

	// print stats periodically
	go func() {
		for {
			time.Sleep(time.Second)
			fmt.Fprintf(io.Discard, "all requests: %d\n", stats.ActualRoundTrips())
		}
	}()

	// will take `upto` requests, with a `timeout` delay between them
	resp, err := hedged.RoundTrip(req)
	if err != nil {
		panic(err)
	}
	defer resp.Body.Close()

	// and do something with resp

	// Output:
}

func Example_instrumented() {
	transport := &InstrumentedTransport{
		Transport: http.DefaultTransport,
	}

	_, err := hedgedhttp.NewRoundTripper(time.Millisecond, 3, transport)
	if err != nil {
		panic(err)
	}

	// Output:
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

func Example_ratelimited() {
	transport := &RateLimitedHedgedTransport{
		Transport: http.DefaultTransport,
		Limiter:   &RandomRateLimiter{},
	}

	_, err := hedgedhttp.NewRoundTripper(time.Millisecond, 3, transport)
	if err != nil {
		panic(err)
	}

	// Output:
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

// Just for the example.
type RandomRateLimiter struct{}

func (r *RandomRateLimiter) Wait(ctx context.Context) error {
	if rand.Int()%2 == 0 {
		return errors.New("rate limit exceed")
	}
	return nil
}

func ExampleMultiTransport() {
	transport := &MultiTransport{
		First:  &http.Transport{MaxIdleConns: 10}, // just an example
		Hedged: &http.Transport{MaxIdleConns: 30}, // just an example
	}

	_, err := hedgedhttp.NewRoundTripper(time.Millisecond, 3, transport)
	if err != nil {
		panic(err)
	}

	// Output:
}

type MultiTransport struct {
	First  http.RoundTripper // Used to make 1st requests.
	Hedged http.RoundTripper // Used to make hedged requests.
}

func (t *MultiTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	if hedgedhttp.IsHedgedRequest(req) {
		return t.Hedged.RoundTrip(req)
	}
	return t.First.RoundTrip(req)
}

type observableRoundTripper struct {
	rt         http.RoundTripper
	maxLatency atomic.Uint64
}

func (ort *observableRoundTripper) MaxLatency() time.Duration {
	return time.Duration(ort.maxLatency.Load())
}

func (ort *observableRoundTripper) RoundTrip(req *http.Request) (*http.Response, error) {
	start := time.Now()
	resp, err := ort.rt.RoundTrip(req)
	if err != nil {
		return resp, err
	}

	took := uint64(time.Since(start).Nanoseconds())
	for {
		max := ort.maxLatency.Load()
		if max >= took {
			return resp, err
		}
		if ort.maxLatency.CompareAndSwap(max, took) {
			return resp, err
		}
	}
}
