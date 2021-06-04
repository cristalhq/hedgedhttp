package hedgedhttp

import (
	"context"
	"net/http"
	"time"
)

// NewClient returns a new http.Client which implements hedged requests pattern.
// Given Client starts a new request after a timeout from previous request.
// Starts no more than upto requests.
// Cancel requests after a successful return if cancel is true.
func NewClient(timeout time.Duration, upto int, cancel bool, client *http.Client) *http.Client {
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
		cancel:  cancel,
	}
	return client
}

// NewRoundTripper returns a new http.RoundTripper which implements hedged requests pattern.
// Given RoundTripper starts a new request after a timeout from previous request.
// Starts no more than upto requests.
// Cancel requests after a successful return if cancel is true.
func NewRoundTripper(timeout time.Duration, upto int, cancel bool, rt http.RoundTripper) http.RoundTripper {
	if rt == nil {
		rt = http.DefaultTransport
	}
	hedged := &hedgedTransport{
		rt:      rt,
		timeout: timeout,
		upto:    upto,
		cancel:  cancel,
	}
	return hedged
}

type hedgedTransport struct {
	rt      http.RoundTripper
	timeout time.Duration
	upto    int
	cancel  bool
}

func (ht *hedgedTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	ctx := req.Context()
	if ht.cancel {
		var cancel func()
		ctx, cancel = context.WithCancel(ctx)
		defer cancel()
		req = req.WithContext(ctx)
	}

	var res interface{}
	resultCh := make(chan interface{}, ht.upto)

	for sent := 0; ; sent++ {
		if sent < ht.upto {
			runInPool(func() {
				resp, err := ht.rt.RoundTrip(req)
				if err != nil {
					resultCh <- err
				} else {
					resultCh <- resp
				}
			})
		}

		select {
		case res = <-resultCh:
		case <-ctx.Done():
			res = ctx.Err()
		case <-time.After(ht.timeout):
			continue
		}
		// either resultCh or ctx.Done is finished
		break
	}

	switch res := res.(type) {
	case error:
		return nil, res
	case *http.Response:
		return res, nil
	default:
		panic("unreachable")
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
