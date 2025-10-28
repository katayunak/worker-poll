package main

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"net/http"
	_ "net/http/pprof"
	"sync"
	"time"
)

type job struct {
	id  int
	num int
}

var (
	jobs         = make(chan *job)
	dlq          = make(chan *job, 100) // DLQ with limited buffer (dead letter queue for jobs that are failed after retries)
	wg           = sync.WaitGroup{}
	workerNumber = 10
	jobNumber    = 1000000
	maxRetries   = 5
)

func init() {
	go server()
}

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	for i := 1; i <= workerNumber; i++ {
		fmt.Println("worker ", i, " is starting ...")
		wg.Add(1)
		go handleJob(ctx, i, jobs, maxRetries)
	}

	for i := 1; i <= jobNumber; i++ {
		jobs <- &job{id: i}
	}

	close(jobs)
	wg.Wait()
	fmt.Println("everything is over ...")
}

func handleJob(ctx context.Context, workerID int, jobs chan *job, maxRetries int) {
	defer wg.Done()

	for {
		select {
		case <-ctx.Done():
			fmt.Println("canceling everything ...")
			return
		case jb, ok := <-jobs:
			if !ok {
				fmt.Println("channel closed for worker ", workerID)
				return
			}
			fmt.Println("job ", jb.id, " started ...")
			var afterRetriesErr error
			for i := 0; i < maxRetries; i++ {
				err := processJob(jb)
				if err == nil {
					afterRetriesErr = nil
					break
				}
				afterRetriesErr = errors.New("job failed")
				time.Sleep(time.Duration((i+1)*2) * time.Millisecond) // backoff
			}
			if afterRetriesErr != nil {
				dlq <- jb
				continue
			}
			fmt.Println("job ", jb.id, " is done ...")
		}
	}
}

func processJob(jb *job) error {
	if rand.Intn(5) == 3 { // ~20% chance fail
		return fmt.Errorf("job %d failed", jb.id)
	}
	time.Sleep(1 * time.Millisecond)
	jb.num = rand.Int()
	return nil
}

func server() {
	http.ListenAndServe("127.0.0.1:6060", nil)
}
