package hedgedhttp

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"io/ioutil"
	"math/rand"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestUpto(t *testing.T) {
	gotRequests := 0

	url := testServerURL(t, func(w http.ResponseWriter, r *http.Request) {
		gotRequests++
		time.Sleep(100 * time.Millisecond)
	})

	req, err := http.NewRequest("GET", url, http.NoBody)
	if err != nil {
		t.Fatal(err)
	}

	const upto = 7
	_, _ = NewClient(10*time.Millisecond, upto, true, nil).Do(req)

	if gotRequests != upto {
		t.Fatalf("want %v, got %v", upto, gotRequests)
	}
}

func TestNoTimeout(t *testing.T) {
	const sleep = 10 * time.Millisecond
	var gotRequests = 0

	url := testServerURL(t, func(w http.ResponseWriter, r *http.Request) {
		gotRequests++
		time.Sleep(sleep)
	})

	req, err := http.NewRequest("GET", url, http.NoBody)
	if err != nil {
		t.Fatal(err)
	}

	const upto = 10

	start := time.Now()
	_, _ = NewClient(0, upto, true, nil).Do(req)
	passed := time.Since(start)

	want := float64(sleep) * 1.5 // some coefficient
	if float64(passed) > want {
		t.Fatalf("want %v, got %v", time.Duration(want), passed)
	}
	if gotRequests != upto {
		t.Fatalf("want %v, got %v", upto, gotRequests)
	}
}

func TestFirstIsOK(t *testing.T) {
	url := testServerURL(t, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})

	req, err := http.NewRequest("GET", url, http.NoBody)
	if err != nil {
		t.Fatal(err)
	}

	resp, err := NewClient(10*time.Millisecond, 10, true, nil).Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	if string(body) != "ok" {
		t.Fatalf("want ok, got %s", string(body))
	}
}

func TestBestResponse(t *testing.T) {
	timeout := []time.Duration{7000 * time.Millisecond, 100 * time.Millisecond, 20 * time.Millisecond}
	shortest := shortestFrom(timeout)

	url := testServerURL(t, func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(timeout[rand.Int()%len(timeout)])
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, "GET", url, http.NoBody)
	if err != nil {
		t.Fatal(err)
	}

	start := time.Now()
	_, _ = NewClient(10*time.Millisecond, 10, true, nil).Do(req)
	passed := time.Since(start)

	if float64(passed) > float64(shortest)*1.2 {
		t.Fatalf("want %v, got %v", shortest, passed)
	}
}

func TestGetSuccessEvenWithErrorsPresent(t *testing.T) {
	var gotRequests uint64

	url := testServerURL(t, func(w http.ResponseWriter, r *http.Request) {
		idx := atomic.AddUint64(&gotRequests, 1)
		if idx == 5 {
			w.WriteHeader(http.StatusOK)
			if _, err := w.Write([]byte("success")); err != nil {
				t.Fatal(err)
			}
			return
		}

		conn, _, err := w.(http.Hijacker).Hijack()
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		_ = conn.Close() // emulate error by closing connection on client side
	})

	req, err := http.NewRequest("GET", url, http.NoBody)
	if err != nil {
		t.Fatal(err)
	}

	resp, err := NewClient(10*time.Millisecond, 5, true, nil).Do(req)
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("Unexpected resp status code: %+v", resp.StatusCode)
	}

	respBytes, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(respBytes, []byte("success")) {
		t.Fatalf("Unexpected resp body %+v; as string: %+v", respBytes, string(respBytes))
	}
}

func TestGetFailureAfterAllRetries(t *testing.T) {
	const upto = 5

	url := testServerURL(t, func(w http.ResponseWriter, r *http.Request) {
		conn, _, err := w.(http.Hijacker).Hijack()
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		_ = conn.Close() // emulate error by closing connection on client side
	})

	req, err := http.NewRequest("GET", url, http.NoBody)
	if err != nil {
		t.Fatal(err)
	}

	resp, err := NewClient(time.Millisecond, upto, true, nil).Do(req)
	if err == nil {
		t.Fatal(err)
	}
	if resp != nil {
		t.Fatalf("Unexpected response %+v", resp)
	}

	wantErrStr := fmt.Sprintf(`%d errors occurred:`, upto)
	if !strings.Contains(err.Error(), wantErrStr) {
		t.Fatalf("Unexpected err %+v", err)
	}
}

func TestHangAllExceptLast(t *testing.T) {
	const upto = 5
	var gotRequests uint64
	blockCh := make(chan struct{})
	defer close(blockCh)

	url := testServerURL(t, func(w http.ResponseWriter, r *http.Request) {
		gotRequests++
		if gotRequests == upto {
			time.Sleep(100 * time.Millisecond)
			return
		}
		<-blockCh
	})

	req, err := http.NewRequest("GET", url, http.NoBody)
	if err != nil {
		t.Fatal(err)
	}

	resp, err := NewClient(10*time.Millisecond, upto, true, nil).Do(req)
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("Unexpected resp status code: %+v", resp.StatusCode)
	}
}

func TestCancelByClient(t *testing.T) {
	blockCh := make(chan struct{})
	defer close(blockCh)

	url := testServerURL(t, func(w http.ResponseWriter, r *http.Request) {
		<-blockCh
	})

	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(50 * time.Millisecond)
		cancel()
	}()

	req, err := http.NewRequestWithContext(ctx, "GET", url, http.NoBody)
	if err != nil {
		t.Fatal(err)
	}

	resp, err := NewClient(10*time.Millisecond, 5, true, nil).Do(req)
	if err == nil {
		t.Fatal(err)
	}
	if resp != nil {
		t.Fatalf("Unexpected resp: %+v", resp)
	}
}

func testServerURL(t *testing.T, h func(http.ResponseWriter, *http.Request)) string {
	server := httptest.NewServer(http.HandlerFunc(h))
	t.Cleanup(server.Close)
	return server.URL
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
