package hedgedhttp

import (
	"context"
	"fmt"
	"net/http"
	"strings"
	"time"
)

const waitForever = 30 * 24 * time.Hour // domain specific forever

// NewClient returns a new http.Client which implements hedged requests pattern.
// Given Client starts a new request after a timeout from previous request.
// Starts no more than upto requests.
func NewClient(timeout time.Duration, upto int, client *http.Client) *http.Client {
	if client == nil {
		client = &http.Client{
			Timeout: 5 * time.Second,
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

	gotErrors := 0
	errOverall := &MultiError{}
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

		resp, err := waitResult(mainCtx, resultCh, errorCh, ht.timeout)

		switch {
		case resp != nil:
			return resp, nil
		case mainCtx.Err() != nil:
			return nil, mainCtx.Err()
		case err != nil:
			gotErrors++
			errOverall.Errors = append(errOverall.Errors, err)
		}
	}

	if gotErrors == ht.upto {
		return nil, errOverall
	}

	resp, err := waitResult(mainCtx, resultCh, errorCh, waitForever)
	if err != nil {
		return nil, combineErrors(errOverall, err, errorCh)
	}
	return resp, nil
}

func waitResult(ctx context.Context, resultCh <-chan *http.Response, errorCh <-chan error, timeout time.Duration) (*http.Response, error) {
	// try to read result first before blocking on all other channels
	select {
	case res := <-resultCh:
		return res, nil
	default:
		// it's okay to return earlier, all the errors will be collected in a buffered channel
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
			return nil, nil // it's not a request timeout, it's timeout BETWEEN consecutive requests
		}
	}
}

func reqWithCtx(r *http.Request, ctx context.Context) (*http.Request, func()) {
	ctx, cancel := context.WithCancel(ctx)
	req := r.WithContext(ctx)
	return req, cancel
}

func combineErrors(errOverall *MultiError, err error, ch <-chan error) *MultiError {
	for {
		select {
		case errCh := <-ch:
			errOverall.Errors = append(errOverall.Errors, errCh)
		default:
			errOverall.Errors = append(errOverall.Errors, err)
			return errOverall
		}
	}
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
