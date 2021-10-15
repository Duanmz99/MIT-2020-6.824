package raft

import "log"

// Debugging
// Set debug to 1 to print the log, set debug to 0 to ignore the log

const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}
