package hedgedhttp

import (
	"context"
	"errors"
	"net/http"
	"time"
)

var errRequestTimeout = errors.New("hedgedhttp: request timeout")

const waitForever = 30 * 24 * time.Hour // domain specific forever

// NewClient returns a new http.Client which implements hedged requests pattern.
// Given Client starts a new request after a timeout from previous request.
// Starts no more than upto requests.
func NewClient(timeout time.Duration, upto int, client *http.Client) *http.Client {
	if client == nil {
		client = &http.Client{
			Timeout: 15 * time.Second,
		}
	}
	if client.Transport == nil {
		client.Transport = http.DefaultTransport
	}

	client.Transport = &hedgedTransport{
		rt:      client.Transport,
		timeout: timeout,
		upto:    upto,
	}
	return client
}

// NewRoundTripper returns a new http.RoundTripper which implements hedged requests pattern.
// Given RoundTripper starts a new request after a timeout from previous request.
// Starts no more than upto requests.
func NewRoundTripper(timeout time.Duration, upto int, rt http.RoundTripper) http.RoundTripper {
	if rt == nil {
		rt = http.DefaultTransport
	}
	hedged := &hedgedTransport{
		rt:      rt,
		timeout: timeout,
		upto:    upto,
	}
	return hedged
}

type hedgedTransport struct {
	rt      http.RoundTripper
	timeout time.Duration
	upto    int
}

func (ht *hedgedTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	mainCtx, mainCtxCancel := context.WithCancel(req.Context())
	defer mainCtxCancel()

	resultCh := make(chan *http.Response, ht.upto)
	errorCh := make(chan error, ht.upto)

	for sent := 0; sent < ht.upto; sent++ {
		runInPool(func() {
			req, cancel := reqWithCtx(req, mainCtx)
			defer cancel()

			resp, err := ht.rt.RoundTrip(req)
			if err != nil {
				errorCh <- err
			} else {
				resultCh <- resp
			}
		})

		// TODO: do not skip error, collect them together
		resp, _ := waitResult(mainCtx, resultCh, errorCh, ht.timeout)

		switch {
		case resp != nil:
			return resp, nil
		case mainCtx.Err() != nil:
			return nil, mainCtx.Err()
		}
	}
	return waitResult(mainCtx, resultCh, errorCh, waitForever)
}

func waitResult(ctx context.Context, resultCh <-chan *http.Response, errorCh <-chan error, timeout time.Duration) (*http.Response, error) {
	// try to read result first before blocking on all other channels
	select {
	case res := <-resultCh:
		return res, nil
	default:
		// it's okay to retyrn earlier, all the errors will be collected in a buffered channel
		if timeout == 0 {
			return nil, nil // nothing to wait, go next iteration
		}

		timer := time.NewTimer(timeout)
		defer timer.Stop()

		select {
		case res := <-resultCh:
			return res, nil

		case reqErr := <-errorCh:
			return nil, reqErr

		case <-ctx.Done():
			return nil, ctx.Err()

		case <-timer.C:
			return nil, errRequestTimeout
		}
	}
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
