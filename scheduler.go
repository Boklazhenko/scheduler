package scheduler

import (
	"context"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"github.com/emirpasic/gods/sets/treeset"
)

const chanBuffSize = 1000
const workerCount = 1000
const inactivityInterval = time.Hour

type Job struct {
	time     int64
	interval time.Duration
	job      func()
	every    bool
	canceled int32
	w        *worker
}

func compare(lhs, rhs interface{}) int {
	return int(lhs.(*Job).time - rhs.(*Job).time)
}

func (j *Job) Cancel() {
	atomic.StoreInt32(&j.canceled, 1)
	j.w.cancelCh <- j
}

type Scheduler struct {
	workers []*worker
}

func New() *Scheduler {
	s := &Scheduler{
		workers: make([]*worker, workerCount),
	}
	for i := 0; i < workerCount; i++ {
		s.workers[i] = newWorker()
	}

	return s
}

func (s *Scheduler) Run(ctx context.Context) {
	wg := sync.WaitGroup{}

	for _, impl := range s.workers {
		wg.Add(1)
		go func(impl *worker) {
			defer wg.Done()
			impl.run(ctx)
		}(impl)
	}

	wg.Wait()
}

func (s *Scheduler) Once(ctx context.Context, interval time.Duration, j func()) (*Job, error) {
	return s.insertJob(ctx, interval, j, false)
}

func (s *Scheduler) Every(ctx context.Context, interval time.Duration, j func()) (*Job, error) {
	return s.insertJob(ctx, interval, j, true)
}

func (s *Scheduler) insertJob(ctx context.Context, interval time.Duration, j func(), every bool) (*Job, error) {
	w := s.workers[rand.Intn(len(s.workers))]
	job := &Job{
		time:     time.Now().UnixNano() + interval.Nanoseconds(),
		interval: interval,
		job:      j,
		every:    every,
		canceled: 0,
		w:        w,
	}

	select {
	case w.insertCh <- job:
		return job, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

type worker struct {
	insertCh chan *Job
	cancelCh chan *Job
}

func newWorker() *worker {
	return &worker{
		insertCh: make(chan *Job, chanBuffSize),
		cancelCh: make(chan *Job, chanBuffSize),
	}
}

func (w *worker) run(ctx context.Context) {
	set := treeset.NewWith(compare)
	timer := time.NewTimer(inactivityInterval)

	for {
		select {
		case now := <-timer.C:
			if set.Empty() {
				timer.Reset(inactivityInterval)
			} else {
				i := set.Iterator()
				i.First()
				j := i.Value().(*Job)

				set.Remove(j)

				if atomic.LoadInt32(&j.canceled) == 0 {
					j.job()

					if j.every {
						j.time = now.UnixNano() + j.interval.Nanoseconds()
						w.insertCh <- j
					}
				}

				i = set.Iterator()
				if i.First() {
					timer.Reset(time.Duration(i.Value().(*Job).time - now.UnixNano()))
				} else {
					timer.Reset(inactivityInterval)
				}
			}
		case j := <-w.insertCh:
			for ; set.Contains(j); j.time++ {
			}
			i := set.Iterator()
			if !i.First() || j.time < i.Value().(*Job).time {
				if !timer.Stop() {
					<-timer.C
				}
				timer.Reset(time.Duration(j.time - time.Now().UnixNano()))
			}
			set.Add(j)
		case j := <-w.cancelCh:
			set.Remove(j)

			i := set.Iterator()

			if i.First() && i.Value().(*Job).time < j.time {
				continue
			}

			if !timer.Stop() {
				<-timer.C
			}

			if i.First() {
				timer.Reset(time.Duration(i.Value().(*Job).time - time.Now().UnixNano()))
			} else {
				timer.Reset(inactivityInterval)
			}
		case <-ctx.Done():
			if !timer.Stop() {
				<-timer.C
			}
			return
		}
	}
}
