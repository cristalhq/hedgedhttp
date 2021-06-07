package hedgedhttp

import (
	"context"
	"fmt"
	"net/http"
	"strings"
	"time"
)

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
	overallRequestCtx, overallRequestCancelFunc := context.WithCancel(req.Context())
	defer overallRequestCancelFunc()

	var res *http.Response
	err := &MultiError{}
	errorsHappened := 0
	resultCh := make(chan *http.Response, ht.upto)
	errorCh := make(chan error, ht.upto)

Loop:
	for sent := 0; errorsHappened < ht.upto; sent++ {
		if sent < ht.upto {
			runInPool(func() {
				subRequestCtx, subRequestCancelFunc := context.WithCancel(overallRequestCtx)
				defer subRequestCancelFunc()
				req := req.WithContext(subRequestCtx)
				resp, err := ht.rt.RoundTrip(req)
				if err != nil {
					errorCh <- err
				} else {
					resultCh <- resp
				}
			})
		}
		// try to read result channel first, before blocking on all other channels
		select {
		case res = <-resultCh:
			break Loop
		default:
		}

		select {
		case res = <-resultCh:
			break Loop
		case subRequestErr := <-errorCh:
			err.Errors = append(err.Errors, subRequestErr)
			errorsHappened++
			continue
		case <-overallRequestCtx.Done():
			err.Errors = append(err.Errors, overallRequestCtx.Err())
		case <-time.After(ht.timeout):
			continue
		}
		break Loop
	}
	if res != nil {
		return res, nil
	}
	if err.ErrorOrNil() != nil {
		return nil, err
	}
	panic("unreachable")
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
