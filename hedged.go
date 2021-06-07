package hedgedhttp

import (
	"context"
	"fmt"
	"net/http"
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
	var err error
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
			err = encodeMultipleErrors(err, subRequestErr)
			errorsHappened++
			continue
		case <-overallRequestCtx.Done():
			err = encodeMultipleErrors(err, overallRequestCtx.Err())
		case <-time.After(ht.timeout):
			continue
		}
		break Loop
	}
	if res != nil {
		return res, nil
	}
	if err != nil {
		return nil, err
	}
	panic("unreachable")
}

func encodeMultipleErrors(err error, subRequestErr error) error {
	if err != nil {
		err = fmt.Errorf("%v\n\t and other error happened `%w`", err, subRequestErr)
	} else {
		err = fmt.Errorf("error happened `%w`", subRequestErr)
	}
	return err
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
