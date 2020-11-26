package scheduler

import (
	"context"
	"github.com/emirpasic/gods/sets/treeset"
	"time"
)

const chanBuffSize = 1000
const inactivityInterval = time.Hour

type job struct {
	t     int64
	i     time.Duration
	j     func()
	s     *Scheduler
	every bool
}

func compare(lhs, rhs interface{}) int {
	return int(lhs.(*job).t - rhs.(*job).t)
}

func (j *job) Cancel() {
	j.s.removeCh <- j
}

type Scheduler struct {
	set      *treeset.Set
	removeCh chan *job
	insertCh chan *job
	timer    *time.Timer
}

func New() *Scheduler {
	return &Scheduler{
		set:      treeset.NewWith(compare),
		removeCh: make(chan *job, chanBuffSize),
		insertCh: make(chan *job, chanBuffSize),
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
				j := i.Value().(*job)
				go j.j()
				s.set.Remove(j)
				if j.every {
					j.t = now.UnixNano() + j.i.Nanoseconds()
					s.insertCh <- j
				}
				i = s.set.Iterator()
				if i.First() {
					s.timer.Reset(time.Duration(i.Value().(*job).t - now.UnixNano()))
				} else {
					s.timer.Reset(inactivityInterval)
				}
			}
		case j := <-s.removeCh:
			i := s.set.Iterator()
			if i.First() {
				if i.Value().(*job).t == j.t {
					if !s.timer.Stop() {
						<-s.timer.C
					}
					if i.Next() {
						s.timer.Reset(time.Duration(i.Value().(*job).t - time.Now().UnixNano()))
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
			if !i.First() || j.t < i.Value().(*job).t {
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

func (s *Scheduler) Once(interval time.Duration, j func()) *job {
	return s.insertJob(interval, j, false)
}

func (s *Scheduler) Every(interval time.Duration, j func()) *job {
	return s.insertJob(interval, j, true)
}

func (s *Scheduler) insertJob(interval time.Duration, j func(), every bool) *job {
	job := &job{
		t:     time.Now().UnixNano() + interval.Nanoseconds(),
		i:     interval,
		j:     j,
		s:     s,
		every: every,
	}
	s.insertCh <- job
	return job
}
