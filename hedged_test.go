package hedgedhttp_test

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cristalhq/hedgedhttp"
)

func TestClient(t *testing.T) {
	const handlerSleep = 100 * time.Millisecond
	url := testServerURL(t, func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(handlerSleep)
	})

	cfg := hedgedhttp.Config{
		Transport: http.DefaultTransport,
		Upto:      3,
		Delay:     50 * time.Millisecond,
		Next: func() (upto int, delay time.Duration) {
			return 5, 10 * time.Millisecond
		},
	}
	client, err := hedgedhttp.New(cfg)
	mustOk(t, err)

	start := time.Now()
	resp, err := client.Do(newGetReq(url))
	took := time.Since(start)
	mustOk(t, err)
	defer resp.Body.Close()
	mustTrue(t, resp != nil)
	mustEqual(t, resp.StatusCode, http.StatusOK)

	stats := client.Stats()
	mustEqual(t, stats.ActualRoundTrips(), uint64(5))
	mustEqual(t, stats.OriginalRequestWins(), uint64(1))
	mustTrue(t, took >= handlerSleep && took < (handlerSleep+10*time.Millisecond))
}

func TestClientBadNextUpto(t *testing.T) {
	const handlerSleep = 100 * time.Millisecond
	url := testServerURL(t, func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(handlerSleep)
	})

	cfg := hedgedhttp.Config{
		Transport: http.DefaultTransport,
		Upto:      2,
		Delay:     50 * time.Millisecond,
		Next: func() (upto int, delay time.Duration) {
			return -1, 10 * time.Millisecond
		},
	}
	client, err := hedgedhttp.New(cfg)
	mustOk(t, err)

	start := time.Now()
	resp, err := client.Do(newGetReq(url))
	took := time.Since(start)
	mustOk(t, err)
	defer resp.Body.Close()
	mustTrue(t, resp != nil)
	mustEqual(t, resp.StatusCode, http.StatusOK)

	stats := client.Stats()
	mustEqual(t, stats.ActualRoundTrips(), uint64(0))
	mustTrue(t, took >= handlerSleep && took < (handlerSleep+10*time.Millisecond))
}

func TestClientBadNextDelay(t *testing.T) {
	const handlerSleep = 100 * time.Millisecond
	url := testServerURL(t, func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(handlerSleep)
	})

	cfg := hedgedhttp.Config{
		Transport: http.DefaultTransport,
		Upto:      2,
		Delay:     150 * time.Millisecond,
		Next: func() (upto int, delay time.Duration) {
			return 2, -10 * time.Millisecond
		},
	}
	client, err := hedgedhttp.New(cfg)
	mustOk(t, err)

	start := time.Now()
	resp, err := client.Do(newGetReq(url))
	took := time.Since(start)
	mustOk(t, err)
	defer resp.Body.Close()
	mustTrue(t, resp != nil)
	mustEqual(t, resp.StatusCode, http.StatusOK)

	stats := client.Stats()
	mustEqual(t, stats.ActualRoundTrips(), uint64(1))
	mustTrue(t, took >= handlerSleep && took < (handlerSleep+10*time.Millisecond))
}

func TestValidateInput(t *testing.T) {
	var err error
	_, err = hedgedhttp.New(hedgedhttp.Config{
		Delay: -time.Second,
	})
	mustFail(t, err)

	_, err = hedgedhttp.New(hedgedhttp.Config{
		Upto: -1,
	})
	mustFail(t, err)

	_, _, err = hedgedhttp.NewClientAndStats(-time.Second, 0, nil)
	mustFail(t, err)

	_, _, err = hedgedhttp.NewClientAndStats(time.Second, -1, nil)
	mustFail(t, err)

	_, _, err = hedgedhttp.NewClientAndStats(time.Second, -1, nil)
	mustFail(t, err)

	_, err = hedgedhttp.NewRoundTripper(time.Second, -1, nil)
	mustFail(t, err)
}

func TestUpto(t *testing.T) {
	var gotRequests int64
	url := testServerURL(t, func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt64(&gotRequests, 1)
		time.Sleep(100 * time.Millisecond)
	})

	const upto = 7
	client, err := hedgedhttp.NewClient(10*time.Millisecond, upto, nil)
	mustOk(t, err)

	resp, err := client.Do(newGetReq(url))
	mustOk(t, err)
	defer resp.Body.Close()

	mustEqual(t, atomic.LoadInt64(&gotRequests), int64(upto))
}

func TestUptoWithInstrumentation(t *testing.T) {
	var gotRequests int64
	url := testServerURL(t, func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt64(&gotRequests, 1)
		time.Sleep(100 * time.Millisecond)
	})

	const upto = 7
	client, metrics, err := hedgedhttp.NewClientAndStats(10*time.Millisecond, upto, nil)
	mustOk(t, err)
	wantZeroMetrics(t, metrics)

	resp, err := client.Do(newGetReq(url))
	mustOk(t, err)
	defer resp.Body.Close()

	mustEqual(t, atomic.LoadInt64(&gotRequests), int64(upto))
	mustEqual(t, metrics.RequestedRoundTrips(), uint64(1))
	mustEqual(t, metrics.ActualRoundTrips(), uint64(upto))
	mustEqual(t, metrics.FailedRoundTrips(), uint64(0))
	mustEqual(t, metrics.CanceledByUserRoundTrips(), uint64(0))
	mustTrue(t, metrics.CanceledSubRequests() <= upto)
}

func TestNoTimeout(t *testing.T) {
	const sleep = 10 * time.Millisecond
	var gotRequests int64

	url := testServerURL(t, func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt64(&gotRequests, 1)
		time.Sleep(sleep)
	})

	const upto = 10
	client, metrics, err := hedgedhttp.NewClientAndStats(0, upto, nil)
	mustOk(t, err)
	wantZeroMetrics(t, metrics)

	resp, err := client.Do(newGetReq(url))
	mustOk(t, err)
	defer resp.Body.Close()

	have := atomic.LoadInt64(&gotRequests)
	mustTrue(t, have >= 1 && have <= upto)
	mustEqual(t, metrics.RequestedRoundTrips(), uint64(1))
	mustTrue(t, metrics.ActualRoundTrips() >= 2 && metrics.ActualRoundTrips() <= upto)
	mustEqual(t, metrics.FailedRoundTrips(), uint64(0))
	mustEqual(t, metrics.CanceledByUserRoundTrips(), uint64(0))
	mustTrue(t, metrics.CanceledSubRequests() <= upto)
}

func TestFirstIsOK(t *testing.T) {
	url := testServerURL(t, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("ok"))
	})

	client, metrics, err := hedgedhttp.NewClientAndStats(10*time.Millisecond, 10, nil)
	mustOk(t, err)

	wantZeroMetrics(t, metrics)
	resp, err := client.Do(newGetReq(url))
	mustOk(t, err)
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	mustOk(t, err)
	mustEqual(t, string(body), "ok")
	mustEqual(t, metrics.RequestedRoundTrips(), uint64(1))
	mustEqual(t, metrics.ActualRoundTrips(), uint64(1))
	mustEqual(t, metrics.FailedRoundTrips(), uint64(0))
	mustEqual(t, metrics.CanceledByUserRoundTrips(), uint64(0))
	mustEqual(t, metrics.CanceledSubRequests(), uint64(0))
}

func TestBestResponse(t *testing.T) {
	const shortest = 20 * time.Millisecond
	timeouts := [...]time.Duration{30 * shortest, 5 * shortest, shortest, shortest, shortest}
	timeoutCh := make(chan time.Duration, len(timeouts))
	for _, t := range timeouts {
		timeoutCh <- t
	}

	url := testServerURL(t, func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(<-timeoutCh)
	})

	start := time.Now()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	const upto = 5
	client, metrics, err := hedgedhttp.NewClientAndStats(10*time.Millisecond, upto, nil)
	mustOk(t, err)
	wantZeroMetrics(t, metrics)

	resp, err := client.Do(newCtxGetReq(ctx, url))
	mustOk(t, err)
	defer resp.Body.Close()

	mustTrue(t, float64(time.Since(start)) <= float64(shortest)*2.5)
	mustEqual(t, metrics.RequestedRoundTrips(), uint64(1))
	mustTrue(t, metrics.ActualRoundTrips() >= upto-1 && metrics.ActualRoundTrips() <= upto)
	mustEqual(t, metrics.FailedRoundTrips(), uint64(0))
	mustEqual(t, metrics.CanceledByUserRoundTrips(), uint64(0))
	mustTrue(t, metrics.CanceledSubRequests() < upto)
}

func TestOriginalResponseWins(t *testing.T) {
	const shortest = 20 * time.Millisecond
	timeouts := [...]time.Duration{shortest, 30 * shortest, 5 * shortest, shortest, shortest, shortest}
	timeoutCh := make(chan time.Duration, len(timeouts))
	for _, t := range timeouts {
		timeoutCh <- t
	}

	url := testServerURL(t, func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(<-timeoutCh)
	})

	start := time.Now()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	const upto = 5
	client, metrics, err := hedgedhttp.NewClientAndStats(10*time.Millisecond, upto, nil)
	mustOk(t, err)

	wantZeroMetrics(t, metrics)
	resp, err := client.Do(newCtxGetReq(ctx, url))
	mustOk(t, err)
	defer resp.Body.Close()

	mustTrue(t, float64(time.Since(start)) <= float64(shortest)*2.5)
	mustEqual(t, metrics.RequestedRoundTrips(), uint64(1))
	mustTrue(t, metrics.ActualRoundTrips() <= 3)
	mustEqual(t, metrics.OriginalRequestWins(), uint64(1))
	mustEqual(t, metrics.HedgedRequestWins(), uint64(0))
	mustEqual(t, metrics.FailedRoundTrips(), uint64(0))
	mustEqual(t, metrics.CanceledByUserRoundTrips(), uint64(0))
	mustTrue(t, metrics.CanceledSubRequests() < upto)
}

func TestHedgedResponseWins(t *testing.T) {
	const shortest = 20 * time.Millisecond
	timeouts := [...]time.Duration{30 * shortest, 5 * shortest, shortest, shortest, shortest}
	timeoutCh := make(chan time.Duration, len(timeouts))
	for _, t := range timeouts {
		timeoutCh <- t
	}

	url := testServerURL(t, func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(<-timeoutCh)
	})

	start := time.Now()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	const upto = 5
	client, metrics, err := hedgedhttp.NewClientAndStats(5*time.Millisecond, upto, nil)
	mustOk(t, err)
	wantZeroMetrics(t, metrics)

	resp, err := client.Do(newCtxGetReq(ctx, url))
	mustOk(t, err)
	defer resp.Body.Close()

	mustTrue(t, float64(time.Since(start)) <= float64(shortest)*2.5)
	mustEqual(t, metrics.RequestedRoundTrips(), uint64(1))
	mustEqual(t, metrics.ActualRoundTrips(), uint64(upto))
	mustEqual(t, metrics.OriginalRequestWins(), uint64(0))
	mustEqual(t, metrics.HedgedRequestWins(), uint64(1))
	mustEqual(t, metrics.FailedRoundTrips(), uint64(0))
	mustEqual(t, metrics.CanceledByUserRoundTrips(), uint64(0))
	mustTrue(t, metrics.CanceledSubRequests() < upto)
}

func TestGetSuccessEvenWithErrorsPresent(t *testing.T) {
	var gotRequests uint64

	const upto = 5
	url := testServerURL(t, func(w http.ResponseWriter, r *http.Request) {
		idx := atomic.AddUint64(&gotRequests, 1)
		if idx == upto {
			w.WriteHeader(http.StatusOK)
			_, err := w.Write([]byte("success"))
			mustOk(t, err)
			return
		}

		conn, _, err := w.(http.Hijacker).Hijack()
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		conn.Close() // emulate error by closing connection on client side
	})

	client, metrics, err := hedgedhttp.NewClientAndStats(10*time.Millisecond, upto, nil)
	mustOk(t, err)

	wantZeroMetrics(t, metrics)
	resp, err := client.Do(newGetReq(url))
	mustOk(t, err)
	defer resp.Body.Close()
	mustEqual(t, resp.StatusCode, http.StatusOK)

	respBytes, err := io.ReadAll(resp.Body)
	mustOk(t, err)
	mustEqual(t, string(respBytes), "success")
	mustEqual(t, metrics.RequestedRoundTrips(), uint64(1))
	mustEqual(t, metrics.ActualRoundTrips(), uint64(upto))
	mustEqual(t, metrics.FailedRoundTrips(), uint64(upto-1))
	mustEqual(t, metrics.CanceledByUserRoundTrips(), uint64(0))
	mustTrue(t, metrics.CanceledSubRequests() <= 4)
}

func TestGetFailureAfterAllRetries(t *testing.T) {
	url := testServerURL(t, func(w http.ResponseWriter, r *http.Request) {
		conn, _, err := w.(http.Hijacker).Hijack()
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		conn.Close() // emulate error by closing connection on client side
	})

	const upto = 5
	client, metrics, err := hedgedhttp.NewClientAndStats(time.Millisecond, upto, nil)
	mustOk(t, err)

	wantZeroMetrics(t, metrics)
	_, err = client.Do(newGetReq(url))
	mustFail(t, err)

	wantErrStr := fmt.Sprintf(`%d errors occurred:`, upto)
	mustTrue(t, strings.Contains(err.Error(), wantErrStr))
	mustEqual(t, metrics.RequestedRoundTrips(), uint64(1))
	mustEqual(t, metrics.ActualRoundTrips(), uint64(upto))
	mustEqual(t, metrics.FailedRoundTrips(), uint64(upto))
	mustEqual(t, metrics.CanceledByUserRoundTrips(), uint64(0))
	mustTrue(t, metrics.CanceledSubRequests() <= upto)
}

func TestHangAllExceptLast(t *testing.T) {
	const upto = 5
	var gotRequests uint64
	blockCh := make(chan struct{})
	defer close(blockCh)

	url := testServerURL(t, func(w http.ResponseWriter, r *http.Request) {
		idx := atomic.AddUint64(&gotRequests, 1)
		if idx == upto {
			time.Sleep(100 * time.Millisecond)
			return
		}
		<-blockCh
	})

	client, metrics, err := hedgedhttp.NewClientAndStats(10*time.Millisecond, upto, nil)
	mustOk(t, err)

	wantZeroMetrics(t, metrics)
	resp, err := client.Do(newGetReq(url))
	mustOk(t, err)
	defer resp.Body.Close()

	mustEqual(t, resp.StatusCode, http.StatusOK)
	mustEqual(t, metrics.RequestedRoundTrips(), uint64(1))
	mustEqual(t, metrics.ActualRoundTrips(), uint64(upto))
	mustEqual(t, metrics.FailedRoundTrips(), uint64(0))
	mustEqual(t, metrics.CanceledByUserRoundTrips(), uint64(0))
	mustTrue(t, metrics.CanceledSubRequests() < upto)
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

	const upto = 5
	client, metrics, err := hedgedhttp.NewClientAndStats(10*time.Millisecond, upto, nil)
	mustOk(t, err)
	wantZeroMetrics(t, metrics)

	_, err = client.Do(newCtxGetReq(ctx, url))
	mustFail(t, err)

	mustEqual(t, metrics.RequestedRoundTrips(), uint64(1))
	mustEqual(t, metrics.ActualRoundTrips(), uint64(upto))
	mustTrue(t, metrics.FailedRoundTrips() < upto)
	mustEqual(t, metrics.CanceledByUserRoundTrips(), uint64(1))
	mustTrue(t, metrics.CanceledSubRequests() <= upto)
}

func TestIsHedged(t *testing.T) {
	var gotRequests int

	rt := testRoundTripper(func(req *http.Request) (*http.Response, error) {
		if gotRequests == 0 {
			mustFalse(t, hedgedhttp.IsHedgedRequest(req))
		} else {
			mustTrue(t, hedgedhttp.IsHedgedRequest(req))
		}
		gotRequests++
		return nil, errors.New("just an error")
	})

	const upto = 7
	client, err := hedgedhttp.NewRoundTripper(10*time.Millisecond, upto, rt)
	mustOk(t, err)

	_, err = client.RoundTrip(newGetReq("http://no-matter-what"))
	mustFail(t, err)
	mustEqual(t, gotRequests, upto)
}

type testRoundTripper func(req *http.Request) (*http.Response, error)

func (t testRoundTripper) RoundTrip(req *http.Request) (*http.Response, error) {
	return t(req)
}

func wantZeroMetrics(tb testing.TB, metrics *hedgedhttp.Stats) {
	tb.Helper()
	mustEqual(tb, metrics.RequestedRoundTrips(), uint64(0))
	mustEqual(tb, metrics.ActualRoundTrips(), uint64(0))
	mustEqual(tb, metrics.FailedRoundTrips(), uint64(0))
	mustEqual(tb, metrics.OriginalRequestWins(), uint64(0))
	mustEqual(tb, metrics.HedgedRequestWins(), uint64(0))
	mustEqual(tb, metrics.CanceledByUserRoundTrips(), uint64(0))
	mustEqual(tb, metrics.CanceledSubRequests(), uint64(0))
}

func testServerURL(tb testing.TB, h func(http.ResponseWriter, *http.Request)) string {
	tb.Helper()
	server := httptest.NewServer(http.HandlerFunc(h))
	tb.Cleanup(server.Close)
	return server.URL
}

func newGetReq(url string) *http.Request {
	req, err := http.NewRequest(http.MethodGet, url, http.NoBody)
	if err != nil {
		panic(err)
	}
	return req
}

func newCtxGetReq(ctx context.Context, url string) *http.Request {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, http.NoBody)
	if err != nil {
		panic(err)
	}
	return req
}

func mustEqual(tb testing.TB, have, want interface{}) {
	tb.Helper()
	if have != want {
		tb.Fatalf("\nhave: %v\nwant: %v\n", have, want)
	}
}

func mustOk(tb testing.TB, err error) {
	tb.Helper()
	if err != nil {
		tb.Fatal(err)
	}
}

func mustFail(tb testing.TB, err error) {
	tb.Helper()
	if err == nil {
		tb.Fatal("want err, got nil")
	}
}

func mustTrue(tb testing.TB, b bool) {
	tb.Helper()
	if !b {
		tb.Fatal()
	}
}

func mustFalse(tb testing.TB, b bool) {
	tb.Helper()
	if b {
		tb.Fatal()
	}
}
