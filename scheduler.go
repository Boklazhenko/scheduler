package scheduler

import (
	"context"
	"github.com/emirpasic/gods/sets/treeset"
	"sync/atomic"
	"time"
)

const chanBuffSize = 1000
const inactivityInterval = time.Hour

type Job struct {
	t        int64
	i        time.Duration
	j        func()
	s        *Scheduler
	every    bool
	canceled int32
}

func compare(lhs, rhs interface{}) int {
	return int(lhs.(*Job).t - rhs.(*Job).t)
}

func (j *Job) Cancel() {
	atomic.StoreInt32(&j.canceled, 1)
	j.s.removeCh <- j
}

type Scheduler struct {
	set      *treeset.Set
	removeCh chan *Job
	insertCh chan *Job
	timer    *time.Timer
}

func New() *Scheduler {
	return &Scheduler{
		set:      treeset.NewWith(compare),
		removeCh: make(chan *Job, chanBuffSize),
		insertCh: make(chan *Job, chanBuffSize),
	}
}

func (s *Scheduler) Run(ctx context.Context) {
	s.timer = time.NewTimer(inactivityInterval)
	for {
		select {
		case now := <-s.timer.C:
			if s.set.Empty() {
				s.timer.Reset(inactivityInterval)
			} else {
				i := s.set.Iterator()
				i.First()
				j := i.Value().(*Job)

				if atomic.LoadInt32(&j.canceled) == 0 {
					go j.j()
				}

				s.set.Remove(j)
				if j.every {
					j.t = now.UnixNano() + j.i.Nanoseconds()
					s.insertCh <- j
				}
				i = s.set.Iterator()
				if i.First() {
					s.timer.Reset(time.Duration(i.Value().(*Job).t - now.UnixNano()))
				} else {
					s.timer.Reset(inactivityInterval)
				}
			}
		case j := <-s.removeCh:
			i := s.set.Iterator()
			if i.First() {
				if i.Value().(*Job).t == j.t {
					if !s.timer.Stop() {
						<-s.timer.C
					}
					if i.Next() {
						s.timer.Reset(time.Duration(i.Value().(*Job).t - time.Now().UnixNano()))
					} else {
						s.timer.Reset(inactivityInterval)
					}
				}
				s.set.Remove(j)
			}
		case j := <-s.insertCh:
			for ; s.set.Contains(j); j.t++ {
			}
			i := s.set.Iterator()
			if !i.First() || j.t < i.Value().(*Job).t {
				if !s.timer.Stop() {
					<-s.timer.C
				}
				s.timer.Reset(time.Duration(j.t - time.Now().UnixNano()))
			}
			s.set.Add(j)
		case <-ctx.Done():
			if !s.timer.Stop() {
				<-s.timer.C
			}
			return
		}
	}
}

func (s *Scheduler) Once(interval time.Duration, j func()) *Job {
	return s.insertJob(interval, j, false)
}

func (s *Scheduler) Every(interval time.Duration, j func()) *Job {
	return s.insertJob(interval, j, true)
}

func (s *Scheduler) insertJob(interval time.Duration, j func(), every bool) *Job {
	job := &Job{
		t:        time.Now().UnixNano() + interval.Nanoseconds(),
		i:        interval,
		j:        j,
		s:        s,
		every:    every,
		canceled: 0,
	}
	s.insertCh <- job
	return job
}
