package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import "os"
import "strconv"

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

// Add your RPC definitions here.
type MRArgs struct {
	Task int   // executed task
	Imap int   // map task id
	Ird  int   // reduce task id
}

const (
	texit int = 0  // task exit
	tmap  int = 1  // task map
	trd   int = 2  // task reduce
	trt   int = 3  // task retry
)

type MRReply struct {
	// global member
	Task int         // one of texit, tmap, trd
	Reduce int       // total number of reduce task
	Nfile int        // total number of files = len(files)
	// task-specific member
	Filename string  // file name to execute(if map task)
	Imap int         // assigned map task
	Ird  int         // assigned reduce task
}


// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
