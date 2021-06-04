package hedgedhttp

import (
	"math/rand"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestUpto(t *testing.T) {
	const upto = 10
	var gotRequests = 0

	h := func(w http.ResponseWriter, r *http.Request) {
		gotRequests++
		time.Sleep(time.Second)
	}
	server := httptest.NewServer(http.HandlerFunc(h))
	t.Cleanup(server.Close)

	req, err := http.NewRequest("GET", server.URL, http.NoBody)
	if err != nil {
		t.Fatal(err)
	}

	c := NewClient(10*time.Millisecond, upto, true, nil)
	c.Do(req)

	if gotRequests != upto {
		t.Fatalf("want %v, got %v", upto, gotRequests)
	}
}

func TestBestResponse(t *testing.T) {
	timeout := []time.Duration{time.Second, 100 * time.Millisecond, 20 * time.Millisecond}
	shortest := shortestFrom(timeout)

	h := func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(timeout[rand.Int()%len(timeout)])
	}
	server := httptest.NewServer(http.HandlerFunc(h))
	t.Cleanup(server.Close)

	req, err := http.NewRequest("GET", server.URL, http.NoBody)
	if err != nil {
		t.Fatal(err)
	}

	c := NewClient(10*time.Millisecond, 10, true, nil)

	start := time.Now()
	c.Do(req)
	passed := time.Since(start)

	if float64(passed) > float64(shortest)*1.2 {
		t.Fatalf("want %v, got %v", shortest, passed)
	}
}

func shortestFrom(ts []time.Duration) time.Duration {
	min := ts[0]
	for _, t := range ts[1:] {
		if t < min {
			min = t
		}
	}
	return min
}
