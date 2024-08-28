package mr

import "log"
import "net"
import "os"
import "net/rpc"
import "net/http"
import "sync"
import "time"

// possible task status
const(
	free int = 0;  	   // not allocated, default
	pending int = 1;   // assigned but not completed
	finished int = 2;  // finished
)



type Coordinator struct {
	// Your definitions here.
	files []string;  // file names(imut)
	nrd int;         // number of reduce(imut)
	smap []int;      // map task status
	dmap int;        // # map task completed
	srd  []int;      // reduce task status
	drd  int;        // # reduce task completed
	// other utility
	lock sync.Mutex;
	timers []time.Time;

}

// mark a task as completed
func (c *Coordinator) Finish(args *MRArgs) error {

	// simply mark corresponding task as completed.
	switch args.Task {
	case tmap: {
		c.timers[args.Imap] = time.Now();
		c.smap[args.Imap] = finished;
		c.dmap++;
	}
	case trd: {
		c.timers[args.Ird] = time.Now();
		c.srd[args.Ird] = finished;
		c.drd++;
	}
	default:  // exit or sleep, don't care.
	}
	return nil;
}

func Timeout(t time.Time) bool {
	dur := time.Since(t);
	var sec float64 = dur.Seconds();
	// timeout after 10.0 seconds;
	return sec > float64(10.25);
}

// assign a task
func (c *Coordinator) Assign(reply *MRReply) error {
	if c.dmap < len(c.files) {
		// in map phase
		tid := -1;
		for i := 0; i < len(c.files); i++ {
			switch c.smap[i] {
			case free: { 
				tid = i; 
				c.timers[i] = time.Now();
				c.smap[i] = pending;
			}
			case pending: {
				if Timeout(c.timers[i]) {
					tid = i;
					c.timers[i] = time.Now();
				}
			}
			}

			if tid >= 0 {
				break;
			}
		}

		// fill in task struct
		reply.Nfile = len(c.files);
		reply.Reduce = c.nrd;
		reply.Imap = tid;
		reply.Ird = -1;
		if tid == -1 {
			reply.Task = trt;
		} else {
			reply.Task = tmap;
			reply.Filename = c.files[tid];
		}
		return nil;
	}

	if c.drd < c.nrd {
		// in reduce phase
		tid := -1;
		for i := 0; i < c.nrd; i++ {
			switch c.srd[i] {
			case free: { 
				tid = i; 
				c.timers[i] = time.Now();
				c.srd[i] = pending;
			}
			case pending: {
				if Timeout(c.timers[i]) {
					tid = i;
					c.timers[i] = time.Now();
				}
			}
			}

			if tid >= 0 {
				break;
			}
		}
		// fill in task struct
		reply.Reduce = c.nrd;
		reply.Nfile = len(c.files);
		reply.Imap = -1;     // not used
		reply.Ird = tid;
		reply.Filename = ""; // not used
		if tid < 0 {
			reply.Task = trt;
		} else {
			reply.Task = trd;
		}
		return nil;
	}

	// else finished, exit
	reply.Task = texit;
	return nil;
}

// Your code here -- RPC handlers for the worker to call.
func (c *Coordinator) Real(args *MRArgs, reply *MRReply) error {
	c.lock.Lock();
	c.Finish(args);
	c.Assign(reply);
	c.lock.Unlock();

	return nil;
}


//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}


//
// start a thread that listens for RPCs from worker.go
//
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	ret := false

	// Your code here.
	c.lock.Lock();  // race!
	ret = c.dmap == len(c.files) && c.drd == c.nrd;
	c.lock.Unlock();


	return ret
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.
	c.files = files;
	c.nrd = nReduce;
	c.smap = make([]int, len(files));
	c.srd = make([]int, nReduce);
	if len(files) < nReduce {
		// unlikely, but possible
		c.timers = make([]time.Time, nReduce);
	} else {
		c.timers = make([]time.Time, len(files));
	}



	c.server()
	return &c
}
