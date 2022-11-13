package scheduler

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/emirpasic/gods/sets/treeset"
)

const chanBuffSize = 10000
const workerCount = 1000
const inactivityInterval = time.Hour

type Job struct {
	time     int64
	interval time.Duration
	job      func()
	every    bool
	canceled int32
}

func compare(lhs, rhs interface{}) int {
	return int(lhs.(*Job).time - rhs.(*Job).time)
}

func (j *Job) Cancel() {
	atomic.StoreInt32(&j.canceled, 1)
}

type Scheduler struct {
	insertCh chan *Job
	impls    []*worker
}

func New() *Scheduler {
	s := &Scheduler{
		insertCh: make(chan *Job, chanBuffSize),
		impls:    make([]*worker, 0),
	}
	for i := 0; i < workerCount; i++ {
		s.impls = append(s.impls, newWorker(s))
	}

	return s
}

func (s *Scheduler) Run(ctx context.Context) {
	wg := sync.WaitGroup{}

	for _, impl := range s.impls {
		wg.Add(1)
		go func(impl *worker) {
			defer wg.Done()
			impl.run(ctx)
		}(impl)
	}

	wg.Wait()
}

func (s *Scheduler) Once(interval time.Duration, j func()) *Job {
	return s.insertJob(interval, j, false)
}

func (s *Scheduler) Every(interval time.Duration, j func()) *Job {
	return s.insertJob(interval, j, true)
}

func (s *Scheduler) insertJob(interval time.Duration, j func(), every bool) *Job {
	job := &Job{
		time:     time.Now().UnixNano() + interval.Nanoseconds(),
		interval: interval,
		job:      j,
		every:    every,
		canceled: 0,
	}
	s.insertCh <- job
	return job
}

type worker struct {
	s *Scheduler
}

func newWorker(s *Scheduler) *worker {
	return &worker{
		s: s,
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
						w.s.insertCh <- j
					}
				}

				i = set.Iterator()
				if i.First() {
					timer.Reset(time.Duration(i.Value().(*Job).time - now.UnixNano()))
				} else {
					timer.Reset(inactivityInterval)
				}
			}
		case j := <-w.s.insertCh:
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
		case <-ctx.Done():
			if !timer.Stop() {
				<-timer.C
			}
			return
		}
	}
}
