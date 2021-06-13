package hedgedhttp

import (
	"context"
	"fmt"
	"net/http"
	"strings"
	"sync/atomic"
	"time"
)

const infiniteTimeout = 30 * 24 * time.Hour // domain specific infinite

// NewClient returns a new http.Client which implements hedged requests pattern.
// Given Client starts a new request after a timeout from previous request.
// Starts no more than upto requests.
func NewClient(timeout time.Duration, upto int, client *http.Client) *http.Client {
	newClient, _ := createNewClient(timeout, upto, client, false)
	return newClient
}

// NewClientWithInstrumentation returns a new http.Client which implements hedged requests pattern
// And TransportInstrumentationMetrics object that can be queried to obtain certain metrics and get better observability.
// Given Client starts a new request after a timeout from previous request.
// Starts no more than upto requests.
func NewClientWithInstrumentation(timeout time.Duration, upto int, client *http.Client) (*http.Client, *TransportInstrumentationMetrics) {
	return createNewClient(timeout, upto, client, true)
}

func createNewClient(timeout time.Duration, upto int, client *http.Client, withInstrumentation bool) (*http.Client, *TransportInstrumentationMetrics) {
	if client == nil {
		client = &http.Client{
			Timeout: 5 * time.Second,
		}
	}
	if withInstrumentation {
		newTransport, metrics := NewRoundTripperWithInstrumentation(timeout, upto, client.Transport)
		client.Transport = newTransport
		return client, metrics
	}
	client.Transport = NewRoundTripper(timeout, upto, client.Transport)
	return client, nil
}

// NewRoundTripperWithInstrumentation returns a new http.RoundTripper which implements hedged requests pattern
// And TransportInstrumentationMetrics object that can be queried to obtain certain metrics and get better observability.
// Given RoundTripper starts a new request after a timeout from previous request.
// Starts no more than upto requests.
func NewRoundTripperWithInstrumentation(timeout time.Duration, upto int, rt http.RoundTripper) (http.RoundTripper, *TransportInstrumentationMetrics) {
	roundTripper := createHedgedRoundTripper(timeout, upto, rt)
	roundTripper.metrics = &TransportInstrumentationMetrics{}
	return roundTripper, roundTripper.metrics
}

// NewRoundTripper returns a new http.RoundTripper which implements hedged requests pattern.
// Given RoundTripper starts a new request after a timeout from previous request.
// Starts no more than upto requests.
func NewRoundTripper(timeout time.Duration, upto int, rt http.RoundTripper) http.RoundTripper {
	return createHedgedRoundTripper(timeout, upto, rt)
}

func createHedgedRoundTripper(timeout time.Duration, upto int, rt http.RoundTripper) *hedgedTransport {
	switch {
	case timeout < 0:
		panic("hedgedhttp: timeout cannot be negative")
	case upto < 1:
		panic("hedgedhttp: upto must be greater than 0")
	}

	if rt == nil {
		rt = http.DefaultTransport
	}

	if timeout == 0 {
		timeout = time.Nanosecond // smallest possible timeout if not set
	}

	hedged := &hedgedTransport{
		rt:      rt,
		timeout: timeout,
		upto:    upto,
	}
	return hedged
}

type freeCacheLine [64]byte

type falseSharingSafeCounter struct {
	count uint64
	_     [7]uint64
}

// TransportInstrumentationMetrics object that can be queried to obtain certain metrics and get better observability.
type TransportInstrumentationMetrics struct {
	_                        freeCacheLine
	requestedRoundTrips      falseSharingSafeCounter
	actualRoundTrips         falseSharingSafeCounter
	failedRoundTrips         falseSharingSafeCounter
	canceledByUserRoundTrips falseSharingSafeCounter
	canceledSubRequests      falseSharingSafeCounter
	_                        freeCacheLine
}

// TransportInstrumentationSnapshot is a snapshot of TransportInstrumentationMetrics.
type TransportInstrumentationSnapshot struct {
	RequestedRoundTrips      uint64 // count of requests that were requested by client
	ActualRoundTrips         uint64 // count of requests that were actually sent
	FailedRoundTrips         uint64 // count of requests that failed
	CanceledByUserRoundTrips uint64 // count of requests that were canceled by user, using request context
	CanceledSubRequests      uint64 // count of hedged sub-requests that were canceled by transport
}

// GetRequestedRoundTrips returns count of requests that were requested by client.
func (m *TransportInstrumentationMetrics) GetRequestedRoundTrips() uint64 {
	return atomic.LoadUint64(&m.requestedRoundTrips.count)
}

// GetActualRoundTrips returns count of requests that were actually sent.
func (m *TransportInstrumentationMetrics) GetActualRoundTrips() uint64 {
	return atomic.LoadUint64(&m.actualRoundTrips.count)
}

// GetFailedRoundTrips returns count of requests that failed.
func (m *TransportInstrumentationMetrics) GetFailedRoundTrips() uint64 {
	return atomic.LoadUint64(&m.failedRoundTrips.count)
}

// GetCanceledByUserRoundTrips returns count of requests that were canceled by user, using request context.
func (m *TransportInstrumentationMetrics) GetCanceledByUserRoundTrips() uint64 {
	return atomic.LoadUint64(&m.canceledByUserRoundTrips.count)
}

// GetCanceledSubRequests returns count of hedged sub-requests that were canceled by transport.
func (m *TransportInstrumentationMetrics) GetCanceledSubRequests() uint64 {
	return atomic.LoadUint64(&m.canceledSubRequests.count)
}

func (m *TransportInstrumentationMetrics) GetSnapshot() TransportInstrumentationSnapshot {
	return TransportInstrumentationSnapshot{
		RequestedRoundTrips:      m.GetRequestedRoundTrips(),
		ActualRoundTrips:         m.GetActualRoundTrips(),
		FailedRoundTrips:         m.GetFailedRoundTrips(),
		CanceledByUserRoundTrips: m.GetCanceledByUserRoundTrips(),
		CanceledSubRequests:      m.GetCanceledSubRequests(),
	}
}

type hedgedTransport struct {
	rt      http.RoundTripper
	timeout time.Duration
	upto    int
	metrics *TransportInstrumentationMetrics
}

func (ht *hedgedTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	mainCtx := req.Context()

	timeout := ht.timeout
	errOverall := &MultiError{}
	resultCh := make(chan indexedResp, ht.upto)
	errorCh := make(chan error, ht.upto)

	if ht.metrics != nil {
		atomic.AddUint64(&ht.metrics.requestedRoundTrips.count, 1)
	}

	resultIdx := -1
	cancels := make([]func(), ht.upto)

	defer runInPool(func() {
		for i, cancel := range cancels {
			if i != resultIdx && cancel != nil {
				if ht.metrics != nil {
					atomic.AddUint64(&ht.metrics.canceledSubRequests.count, 1)
				}
				cancel()
			}
		}
	})

	for sent := 0; len(errOverall.Errors) < ht.upto; sent++ {
		if sent < ht.upto {
			idx := sent
			subReq, cancel := reqWithCtx(req, mainCtx)
			cancels[idx] = cancel

			runInPool(func() {
				if ht.metrics != nil {
					atomic.AddUint64(&ht.metrics.actualRoundTrips.count, 1)
				}
				resp, err := ht.rt.RoundTrip(subReq)
				if err != nil {
					if ht.metrics != nil {
						atomic.AddUint64(&ht.metrics.failedRoundTrips.count, 1)
					}
					errorCh <- err
				} else {
					resultCh <- indexedResp{idx, resp}
				}
			})
		}

		// all request sent - effectively disabling timeout between requests
		if sent == ht.upto {
			timeout = infiniteTimeout
		}
		resp, err := waitResult(mainCtx, resultCh, errorCh, timeout)

		switch {
		case resp.Resp != nil:
			resultIdx = resp.Index
			return resp.Resp, nil
		case mainCtx.Err() != nil:
			if ht.metrics != nil {
				atomic.AddUint64(&ht.metrics.canceledByUserRoundTrips.count, 1)
			}
			return nil, mainCtx.Err()
		case err != nil:
			errOverall.Errors = append(errOverall.Errors, err)
		}
	}

	// all request have returned errors
	return nil, errOverall
}

func waitResult(ctx context.Context, resultCh <-chan indexedResp, errorCh <-chan error, timeout time.Duration) (indexedResp, error) {
	// try to read result first before blocking on all other channels
	select {
	case res := <-resultCh:
		return res, nil
	default:
		timer := time.NewTimer(timeout)
		defer timer.Stop()

		select {
		case res := <-resultCh:
			return res, nil

		case reqErr := <-errorCh:
			return indexedResp{}, reqErr

		case <-ctx.Done():
			return indexedResp{}, ctx.Err()

		case <-timer.C:
			return indexedResp{}, nil // it's not a request timeout, it's timeout BETWEEN consecutive requests
		}
	}
}

type indexedResp struct {
	Index int
	Resp  *http.Response
}

func reqWithCtx(r *http.Request, ctx context.Context) (*http.Request, func()) {
	ctx, cancel := context.WithCancel(ctx)
	req := r.WithContext(ctx)
	return req, cancel
}

var taskQueue = make(chan func())

func runInPool(task func()) {
	select {
	case taskQueue <- task:
		// submited, everything is ok

	default:
		go func() {
			// do the given task
			task()

			const cleanupDuration = 10 * time.Second
			cleanupTicker := time.NewTicker(cleanupDuration)
			defer cleanupTicker.Stop()

			for {
				select {
				case t := <-taskQueue:
					t()
					cleanupTicker.Reset(cleanupDuration)
				case <-cleanupTicker.C:
					return
				}
			}
		}()
	}
}

// MultiError is an error type to track multiple errors. This is used to
// accumulate errors in cases and return them as a single "error".
// Insiper by https://github.com/hashicorp/go-multierror
type MultiError struct {
	Errors        []error
	ErrorFormatFn ErrorFormatFunc
}

func (e *MultiError) Error() string {
	fn := e.ErrorFormatFn
	if fn == nil {
		fn = listFormatFunc
	}
	return fn(e.Errors)
}

func (e *MultiError) String() string {
	return fmt.Sprintf("*%#v", e.Errors)
}

// ErrorOrNil returns an error if there are some.
func (e *MultiError) ErrorOrNil() error {
	switch {
	case e == nil || len(e.Errors) == 0:
		return nil
	default:
		return e
	}
}

// ErrorFormatFunc is called by MultiError to return the list of errors as a string.
type ErrorFormatFunc func([]error) string

func listFormatFunc(es []error) string {
	if len(es) == 1 {
		return fmt.Sprintf("1 error occurred:\n\t* %s\n\n", es[0])
	}

	points := make([]string, len(es))
	for i, err := range es {
		points[i] = fmt.Sprintf("* %s", err)
	}

	return fmt.Sprintf("%d errors occurred:\n\t%s\n\n", len(es), strings.Join(points, "\n\t"))
}
