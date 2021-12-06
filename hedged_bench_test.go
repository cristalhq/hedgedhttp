package hedgedhttp_test

import (
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cristalhq/hedgedhttp"
)

var localRandom = sync.Pool{
	New: func() interface{} {
		return rand.New(rand.NewSource(time.Now().Unix()))
	},
}

func getLocalRand() *rand.Rand {
	return localRandom.Get().(*rand.Rand)
}

func returnLocalRand(rnd *rand.Rand) {
	localRandom.Put(rnd)
}

type FuncRoundTripper struct {
	f func(request *http.Request) (*http.Response, error)
}

func (f *FuncRoundTripper) RoundTrip(request *http.Request) (*http.Response, error) {
	return f.f(request)
}

func BenchmarkHedgedRequest(b *testing.B) {
	benchmarks := []struct {
		concurrency int
	}{
		{concurrency: 1},
		{concurrency: 2},
		{concurrency: 4},
		{concurrency: 8},
		{concurrency: 12},
		{concurrency: 16},
		{concurrency: 24},
		{concurrency: 32},
	}
	for _, bm := range benchmarks {
		b.Run(fmt.Sprintf("concurrency-%v", bm.concurrency), func(b *testing.B) {
			b.ReportAllocs()

			target := &FuncRoundTripper{
				f: func(request *http.Request) (*http.Response, error) {
					rnd := getLocalRand()
					defer returnLocalRand(rnd)

					if rnd.Float32() < 0.3 {
						return &http.Response{}, nil
					}
					return nil, io.EOF
				},
			}

			var errors = uint64(0)
			var snapshot atomic.Value

			hedgedTarget, metrics, err := hedgedhttp.NewRoundTripperAndStats(10*time.Nanosecond, 10, target)
			if err != nil {
				b.Fatalf("want nil, got %s", err)
			}

			initialSnapshot := metrics.Snapshot()
			snapshot.Store(&initialSnapshot)

			go func() {
				ticker := time.NewTicker(1 * time.Millisecond)
				defer ticker.Stop()
				for range ticker.C {
					currentSnapshot := metrics.Snapshot()
					snapshot.Store(&currentSnapshot)
				}
			}()
			req, err := http.NewRequest("GET", "", http.NoBody)
			if err != nil {
				b.Fatal(err)
			}

			var wg sync.WaitGroup
			wg.Add(bm.concurrency)
			for i := 0; i < bm.concurrency; i++ {
				go func() {
					for i := 0; i < b.N/bm.concurrency; i++ {
						_, errRes := hedgedTarget.RoundTrip(req)
						if errRes != nil {
							atomic.AddUint64(&errors, 1)
						}
					}
					wg.Done()
				}()
			}
			wg.Wait()
			if rand.Float32() < 0.001 {
				fmt.Printf("Snapshot: %+v\n", snapshot.Load())
			}
		})
	}
}
