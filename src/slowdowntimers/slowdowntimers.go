package slowdowntimers

import (
	"log"
	"runtime"
	"time"
)

// var BeginFlag bool = false

type SlowdownTimers struct {
	Id int32

	initialized bool
	fired       bool
	timer       *time.Timer

	timeToInject time.Time
	duration     time.Duration
}

// Initialize the timers if they are not initialized.
func (st *SlowdownTimers) InitializeTimers(Id_ int32, time_ time.Time, duration_ time.Duration) {
	if st.initialized {
		return
	}
	log.Printf("slowdowntimers: Initializing slowdown timers for %v\n", Id_)
	st.Id = Id_
	st.timeToInject = time_
	st.duration = duration_

	until := time.Until(st.timeToInject)
	st.timer = &time.Timer{} // we check anyway don't want to segfault
	st.timer = time.NewTimer(until)
	// if until > 0 {
	// 	st.timer = time.NewTimer(until)
	// } else {
	// 	log.Printf("Missed a slowdown time! %v now %v\n", st.timeToInject, time.Now())
	// }

	st.initialized = true
}

// if it blocked on a network read and passed the slowdown time it was like it was slow
// so don't add additional slowdown time.
func (st *SlowdownTimers) CheckAndDoSlowdown() {
	if !st.initialized {
		return
	}
	select {
	case <-st.timer.C:
		pc, _, no, ok := runtime.Caller(1)
		if ok {
			log.Printf("CheckAndDoSlowdown called from %v line %v\n", runtime.FuncForPC(pc).Name(), no)
		}

		d := time.Until(st.timeToInject.Add(time.Duration(st.duration)))
		log.Printf("slowdowntimers: Replica %v: Injecting a slowdown for %v requested %v at time %v\n",
			st.Id, d, st.duration, st.timeToInject)
		time.Sleep(d)
	default:
	}
}

func (st *SlowdownTimers) CheckAndDoLongLivedSlowdown() {
	if !st.initialized {
		return
	}
	if st.fired {
		time.Sleep(st.duration)
		return
	}
	select {
	case <-st.timer.C:
		_, _, no, ok := runtime.Caller(1)
		if ok {
			log.Printf("CheckAndDoLongLivedSlowdown called from %v\n", no)
		}
		st.fired = true
		log.Printf("slowdowntimers: Replica %v: First injection of long-lived slowdown for %v at time %v\n",
			st.Id, st.duration, st.timeToInject)
		time.Sleep(st.duration)
	default:
	}
}
