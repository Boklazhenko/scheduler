package scheduler

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/emirpasic/gods/sets/treeset"
)

const chanBuffSize = 10000
const implCount = 10
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
	impls    []*implementaion
}

func New() *Scheduler {
	s := &Scheduler{
		insertCh: make(chan *Job, chanBuffSize),
		impls:    make([]*implementaion, 0),
	}
	for i := 0; i < implCount; i++ {
		s.impls = append(s.impls, newImpl(s))
	}

	return s
}

func (s *Scheduler) Run(ctx context.Context) {
	wg := sync.WaitGroup{}

	for _, impl := range s.impls {
		wg.Add(1)
		go func(impl *implementaion) {
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

func (s *Scheduler) WaitRunningJobs() {
	for _, i := range s.impls {
		i.waitRunningJobs()
	}
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

type implementaion struct {
	s  *Scheduler
	wg sync.WaitGroup
}

func newImpl(s *Scheduler) *implementaion {
	return &implementaion{
		s: s,
	}
}

func (impl *implementaion) run(ctx context.Context) {
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
					impl.wg.Add(1)
					go func() {
						defer impl.wg.Done()
						j.job()
					}()

					if j.every {
						j.time = now.UnixNano() + j.interval.Nanoseconds()
						impl.s.insertCh <- j
					}
				}

				i = set.Iterator()
				if i.First() {
					timer.Reset(time.Duration(i.Value().(*Job).time - now.UnixNano()))
				} else {
					timer.Reset(inactivityInterval)
				}
			}
		case j := <-impl.s.insertCh:
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

func (impl *implementaion) waitRunningJobs() {
	impl.wg.Wait()
}
