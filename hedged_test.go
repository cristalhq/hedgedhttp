package hedgedhttp

import (
	"bytes"
	"io/ioutil"
	"math/rand"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
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

	c := NewClient(10*time.Millisecond, upto, nil)
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

	c := NewClient(10*time.Millisecond, 10, nil)

	start := time.Now()
	c.Do(req)
	passed := time.Since(start)

	if float64(passed) > float64(shortest)*1.2 {
		t.Fatalf("want %v, got %v", shortest, passed)
	}
}

func TestGetSuccessEvenWithErrorsPresent(t *testing.T) {
	var handlerCount uint64 = 0
	h := func(w http.ResponseWriter, r *http.Request) {
		hj, ok := w.(http.Hijacker)
		if !ok {
			http.Error(w, "webserver doesn't support hijacking", http.StatusInternalServerError)
			return
		}

		idx := atomic.AddUint64(&handlerCount, 1)
		if idx == 5 {
			w.WriteHeader(200)
			_, err := w.Write([]byte("success"))
			if err != nil {
				t.Fatal(err)
			}
			return
		}

		conn, _, err := hj.Hijack()
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		_ = conn.Close() // emulate error by closing connection on client side
	}
	server := httptest.NewServer(http.HandlerFunc(h))
	t.Cleanup(server.Close)

	req, err := http.NewRequest("GET", server.URL, http.NoBody)
	if err != nil {
		t.Fatal(err)
	}

	c := NewClient(10*time.Millisecond, 5, nil)
	response, err := c.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	if response.StatusCode != 200 {
		t.Fatalf("Unexpected resp status code: %+v", response.StatusCode)
	}
	responseBytes, err := ioutil.ReadAll(response.Body)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(responseBytes, []byte("success")) {
		t.Fatalf("Unexpected resp body %+v; as string: %+v", responseBytes, string(responseBytes))
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
