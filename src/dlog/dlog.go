package dlog

import (
	"log"
)

const DLOG = false

func Printf(format string, v ...interface{}) {
	if !DLOG {
		return
	}
	// fmt.Printf("%v: ", time.Now())
	log.Printf(format, v...)
}

func Println(v ...interface{}) {
	if !DLOG {
		return
	}
	// fmt.Printf("%v: ", time.Now())
	log.Println(v...)
}
