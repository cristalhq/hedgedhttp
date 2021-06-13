package hedgedhttp_test

import (
	"bytes"
	"context"
	"fmt"
	"github.com/cristalhq/hedgedhttp"
	"io"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestValidateInput(t *testing.T) {
	f := func(f func()) {
		defer func() {
			if r := recover(); r == nil {
				t.Fatal()
			}
		}()

		f()
	}

	f(func() {
		hedgedhttp.NewClient(-time.Second, 0, nil)
	})
	f(func() {
		hedgedhttp.NewClient(time.Second, -1, nil)
	})
	f(func() {
		hedgedhttp.NewClient(time.Second, 0, nil)
	})
	f(func() {
		hedgedhttp.NewClientWithInstrumentation(-time.Second, 0, nil)
	})
	f(func() {
		hedgedhttp.NewClientWithInstrumentation(time.Second, -1, nil)
	})
	f(func() {
		hedgedhttp.NewClientWithInstrumentation(time.Second, 0, nil)
	})
}

func TestUpto(t *testing.T) {
	var gotRequests int64

	url := testServerURL(t, func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt64(&gotRequests, 1)
		time.Sleep(100 * time.Millisecond)
	})

	req, err := http.NewRequest("GET", url, http.NoBody)
	if err != nil {
		t.Fatal(err)
	}

	const upto = 7
	_, _ = hedgedhttp.NewClient(10*time.Millisecond, upto, nil).Do(req)

	if gotRequests := atomic.LoadInt64(&gotRequests); gotRequests != upto {
		t.Fatalf("want %v, got %v", upto, gotRequests)
	}
}

func TestUptoWithInstrumentation(t *testing.T) {
	var gotRequests int64

	url := testServerURL(t, func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt64(&gotRequests, 1)
		time.Sleep(100 * time.Millisecond)
	})

	req, err := http.NewRequest("GET", url, http.NoBody)
	if err != nil {
		t.Fatal(err)
	}

	const upto = 7
	client, metrics := hedgedhttp.NewClientWithInstrumentation(10*time.Millisecond, upto, nil)
	checkAllMetricsAreZero(t, metrics)

	_, _ = client.Do(req)
	if gotRequests := atomic.LoadInt64(&gotRequests); gotRequests != upto {
		t.Fatalf("want %v, got %v", upto, gotRequests)
	}
	if requestedRoundTrips := metrics.GetRequestedRoundTrips(); requestedRoundTrips != 1 {
		t.Fatalf("Unnexpected requestedRoundTrips: %v", requestedRoundTrips)
	}
	if actualRoundTrips := metrics.GetActualRoundTrips(); actualRoundTrips != upto {
		t.Fatalf("Unnexpected actualRoundTrips: %v", actualRoundTrips)
	}
	if failedRoundTrips := metrics.GetFailedRoundTrips(); failedRoundTrips != 0 {
		t.Fatalf("Unnexpected failedRoundTrips: %v", failedRoundTrips)
	}
	if canceledByUserRoundTrips := metrics.GetCanceledByUserRoundTrips(); canceledByUserRoundTrips != 0 {
		t.Fatalf("Unnexpected canceledByUserRoundTrips: %v", canceledByUserRoundTrips)
	}
	if canceledSubRequests := metrics.GetCanceledSubRequests(); canceledSubRequests > upto {
		t.Fatalf("Unnexpected canceledSubRequests: %v", canceledSubRequests)
	}
}

func TestNoTimeout(t *testing.T) {
	const sleep = 10 * time.Millisecond
	var gotRequests int64

	url := testServerURL(t, func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt64(&gotRequests, 1)
		time.Sleep(sleep)
	})

	req, err := http.NewRequest("GET", url, http.NoBody)
	if err != nil {
		t.Fatal(err)
	}

	const upto = 10

	client, metrics := hedgedhttp.NewClientWithInstrumentation(0, upto, nil)
	checkAllMetricsAreZero(t, metrics)
	_, _ = client.Do(req)

	if gotRequests := atomic.LoadInt64(&gotRequests); gotRequests < 1 || gotRequests > upto {
		t.Fatalf("want %v, got %v", upto, gotRequests)
	}
	if requestedRoundTrips := metrics.GetRequestedRoundTrips(); requestedRoundTrips != 1 {
		t.Fatalf("Unnexpected requestedRoundTrips: %v", requestedRoundTrips)
	}
	if actualRoundTrips := metrics.GetActualRoundTrips(); actualRoundTrips < 2 || actualRoundTrips > upto {
		t.Fatalf("Unnexpected actualRoundTrips: %v", actualRoundTrips)
	}
	if failedRoundTrips := metrics.GetFailedRoundTrips(); failedRoundTrips != 0 {
		t.Fatalf("Unnexpected failedRoundTrips: %v", failedRoundTrips)
	}
	if canceledByUserRoundTrips := metrics.GetCanceledByUserRoundTrips(); canceledByUserRoundTrips != 0 {
		t.Fatalf("Unnexpected canceledByUserRoundTrips: %v", canceledByUserRoundTrips)
	}
	if canceledSubRequests := metrics.GetCanceledSubRequests(); canceledSubRequests > upto {
		t.Fatalf("Unnexpected canceledSubRequests: %v", canceledSubRequests)
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

	client, metrics := hedgedhttp.NewClientWithInstrumentation(10*time.Millisecond, 10, nil)
	checkAllMetricsAreZero(t, metrics)
	resp, err := client.Do(req)
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
	expectExactMetricsAndSnapshot(t, metrics, hedgedhttp.TransportInstrumentationSnapshot{
		RequestedRoundTrips:      1,
		ActualRoundTrips:         1,
		FailedRoundTrips:         0,
		CanceledByUserRoundTrips: 0,
		CanceledSubRequests:      0,
	})
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

	req, err := http.NewRequestWithContext(ctx, "GET", url, http.NoBody)
	if err != nil {
		t.Fatal(err)
	}
	client, metrics := hedgedhttp.NewClientWithInstrumentation(10*time.Millisecond, 5, nil)
	checkAllMetricsAreZero(t, metrics)
	_, err = client.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	passed := time.Since(start)

	if float64(passed) > float64(shortest)*2.5 {
		t.Fatalf("want %v, got %v", shortest, passed)
	}
	if requestedRoundTrips := metrics.GetRequestedRoundTrips(); requestedRoundTrips != 1 {
		t.Fatalf("Unnexpected requestedRoundTrips: %v", requestedRoundTrips)
	}
	if actualRoundTrips := metrics.GetActualRoundTrips(); actualRoundTrips < 4 || actualRoundTrips > 5 {
		t.Fatalf("Unnexpected actualRoundTrips: %v", actualRoundTrips)
	}
	if failedRoundTrips := metrics.GetFailedRoundTrips(); failedRoundTrips != 0 {
		t.Fatalf("Unnexpected failedRoundTrips: %v", failedRoundTrips)
	}
	if canceledByUserRoundTrips := metrics.GetCanceledByUserRoundTrips(); canceledByUserRoundTrips != 0 {
		t.Fatalf("Unnexpected canceledByUserRoundTrips: %v", canceledByUserRoundTrips)
	}
	if canceledSubRequests := metrics.GetCanceledSubRequests(); canceledSubRequests > 4 {
		t.Fatalf("Unnexpected canceledSubRequests: %v", canceledSubRequests)
	}
}

func TestGetSuccessEvenWithErrorsPresent(t *testing.T) {
	var gotRequests uint64

	upto := uint64(5)
	url := testServerURL(t, func(w http.ResponseWriter, r *http.Request) {
		idx := atomic.AddUint64(&gotRequests, 1)
		if idx == upto {
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

	client, metrics := hedgedhttp.NewClientWithInstrumentation(10*time.Millisecond, int(upto), nil)
	checkAllMetricsAreZero(t, metrics)
	resp, err := client.Do(req)
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
	if requestedRoundTrips := metrics.GetRequestedRoundTrips(); requestedRoundTrips != 1 {
		t.Fatalf("Unnexpected requestedRoundTrips: %v", requestedRoundTrips)
	}
	if actualRoundTrips := metrics.GetActualRoundTrips(); actualRoundTrips != upto {
		t.Fatalf("Unnexpected actualRoundTrips: %v", actualRoundTrips)
	}
	if failedRoundTrips := metrics.GetFailedRoundTrips(); failedRoundTrips != upto-1 {
		t.Fatalf("Unnexpected failedRoundTrips: %v", failedRoundTrips)
	}
	if canceledByUserRoundTrips := metrics.GetCanceledByUserRoundTrips(); canceledByUserRoundTrips != 0 {
		t.Fatalf("Unnexpected canceledByUserRoundTrips: %v", canceledByUserRoundTrips)
	}
	if canceledSubRequests := metrics.GetCanceledSubRequests(); canceledSubRequests > 4 {
		t.Fatalf("Unnexpected canceledSubRequests: %v", canceledSubRequests)
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

	client, metrics := hedgedhttp.NewClientWithInstrumentation(time.Millisecond, upto, nil)
	checkAllMetricsAreZero(t, metrics)
	resp, err := client.Do(req)
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
	if requestedRoundTrips := metrics.GetRequestedRoundTrips(); requestedRoundTrips != 1 {
		t.Fatalf("Unnexpected requestedRoundTrips: %v", requestedRoundTrips)
	}
	if actualRoundTrips := metrics.GetActualRoundTrips(); actualRoundTrips != upto {
		t.Fatalf("Unnexpected actualRoundTrips: %v", actualRoundTrips)
	}
	if failedRoundTrips := metrics.GetFailedRoundTrips(); failedRoundTrips != upto {
		t.Fatalf("Unnexpected failedRoundTrips: %v", failedRoundTrips)
	}
	if canceledByUserRoundTrips := metrics.GetCanceledByUserRoundTrips(); canceledByUserRoundTrips != 0 {
		t.Fatalf("Unnexpected canceledByUserRoundTrips: %v", canceledByUserRoundTrips)
	}
	if canceledSubRequests := metrics.GetCanceledSubRequests(); canceledSubRequests > upto {
		t.Fatalf("Unnexpected canceledSubRequests: %v", canceledSubRequests)
	}
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

	req, err := http.NewRequest("GET", url, http.NoBody)
	if err != nil {
		t.Fatal(err)
	}

	client, metrics := hedgedhttp.NewClientWithInstrumentation(10*time.Millisecond, upto, nil)
	checkAllMetricsAreZero(t, metrics)
	resp, err := client.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("Unexpected resp status code: %+v", resp.StatusCode)
	}
	if requestedRoundTrips := metrics.GetRequestedRoundTrips(); requestedRoundTrips != 1 {
		t.Fatalf("Unnexpected requestedRoundTrips: %v", requestedRoundTrips)
	}
	if actualRoundTrips := metrics.GetActualRoundTrips(); actualRoundTrips != upto {
		t.Fatalf("Unnexpected actualRoundTrips: %v", actualRoundTrips)
	}
	if failedRoundTrips := metrics.GetFailedRoundTrips(); failedRoundTrips != 0 {
		t.Fatalf("Unnexpected failedRoundTrips: %v", failedRoundTrips)
	}
	if canceledByUserRoundTrips := metrics.GetCanceledByUserRoundTrips(); canceledByUserRoundTrips != 0 {
		t.Fatalf("Unnexpected canceledByUserRoundTrips: %v", canceledByUserRoundTrips)
	}
	if canceledSubRequests := metrics.GetCanceledSubRequests(); canceledSubRequests > upto-1 {
		t.Fatalf("Unnexpected canceledSubRequests: %v", canceledSubRequests)
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

	upto := 5
	client, metrics := hedgedhttp.NewClientWithInstrumentation(10*time.Millisecond, upto, nil)
	checkAllMetricsAreZero(t, metrics)
	resp, err := client.Do(req)
	if err == nil {
		t.Fatal(err)
	}
	if resp != nil {
		t.Fatalf("Unexpected resp: %+v", resp)
	}
	if requestedRoundTrips := metrics.GetRequestedRoundTrips(); requestedRoundTrips != 1 {
		t.Fatalf("Unnexpected requestedRoundTrips: %v", requestedRoundTrips)
	}
	if actualRoundTrips := metrics.GetActualRoundTrips(); actualRoundTrips != uint64(upto) {
		t.Fatalf("Unnexpected actualRoundTrips: %v", actualRoundTrips)
	}
	if failedRoundTrips := metrics.GetFailedRoundTrips(); failedRoundTrips > uint64(upto) {
		t.Fatalf("Unnexpected failedRoundTrips: %v", failedRoundTrips)
	}
	if canceledByUserRoundTrips := metrics.GetCanceledByUserRoundTrips(); canceledByUserRoundTrips != 1 {
		t.Fatalf("Unnexpected canceledByUserRoundTrips: %v", canceledByUserRoundTrips)
	}
	if canceledSubRequests := metrics.GetCanceledSubRequests(); canceledSubRequests > uint64(upto) {
		t.Fatalf("Unnexpected canceledSubRequests: %v", canceledSubRequests)
	}
}

func checkAllMetricsAreZero(t *testing.T, metrics *hedgedhttp.TransportInstrumentationMetrics) {
	expectExactMetricsAndSnapshot(t, metrics, hedgedhttp.TransportInstrumentationSnapshot{})
}

func expectExactMetricsAndSnapshot(t *testing.T, metrics *hedgedhttp.TransportInstrumentationMetrics, snapshot hedgedhttp.TransportInstrumentationSnapshot) {
	if metrics == nil {
		t.Fatalf("Metrics object can't be nil")
	}
	if requestedRoundTrips := metrics.GetRequestedRoundTrips(); requestedRoundTrips != snapshot.RequestedRoundTrips {
		t.Fatalf("Unnexpected requestedRoundTrips: %+v; expected: %+v", requestedRoundTrips, snapshot.RequestedRoundTrips)
	}
	if actualRoundTrips := metrics.GetActualRoundTrips(); actualRoundTrips != snapshot.ActualRoundTrips {
		t.Fatalf("Unnexpected actualRoundTrips: %+v; expected: %+v", actualRoundTrips, snapshot.ActualRoundTrips)
	}
	if failedRoundTrips := metrics.GetFailedRoundTrips(); failedRoundTrips != snapshot.FailedRoundTrips {
		t.Fatalf("Unnexpected failedRoundTrips: %+v; expected: %+v", failedRoundTrips, snapshot.FailedRoundTrips)
	}
	if canceledByUserRoundTrips := metrics.GetCanceledByUserRoundTrips(); canceledByUserRoundTrips != snapshot.CanceledByUserRoundTrips {
		t.Fatalf("Unnexpected canceledByUserRoundTrips: %+v; expected: %+v", canceledByUserRoundTrips, snapshot.CanceledByUserRoundTrips)
	}
	if canceledSubRequests := metrics.GetCanceledSubRequests(); canceledSubRequests != snapshot.CanceledSubRequests {
		t.Fatalf("Unnexpected canceledSubRequests: %+v; expected: %+v", canceledSubRequests, snapshot.CanceledSubRequests)
	}
	if currentSnapshot := metrics.GetSnapshot(); currentSnapshot != snapshot {
		t.Fatalf("Unnexpected currentSnapshot: %+v; expected: %+v", currentSnapshot, snapshot)
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
