package mr

import "fmt"
import "log"
import "net/rpc"
import "hash/fnv"
import "encoding/json"
import "os"
import "sort"
import "time"
import "io/ioutil"


//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

type MrFunc struct {
	mapf func(string, string) []KeyValue
	reducef func(string, []string) string
}

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()
	fn := MrFunc{ mapf, reducef };

	rp := MRReply{};
	rp.Task = texit;

	for {
		CallReal(fn, &rp);
	}
}

// for sorting by key.
type ByKey []KeyValue;

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

func MapExec(reply *MRReply, mapf func(string, string) []KeyValue) {
	// filename string, assign int, rd int
	filename := reply.Filename;
	assign, rd := reply.Imap, reply.Reduce;

	file, err := os.Open(filename);
	if err != nil {
		log.Fatalf("cannot open %v", filename);
		return;
	}
	content, err := ioutil.ReadAll(file);
	if err != nil {
		log.Fatalf("cannot read %v", filename);
		return;
	}
	file.Close();
	kva := mapf(filename, string(content));

	// hash the kv pairs into rd buckets
	intermediates := make([][]KeyValue, rd);
	for _, kv := range kva {
		code := ihash(kv.Key) % rd;
		intermediates[code] = append(intermediates[code], kv);
	}

	// dump the statistics into rd files
	for i := 0; i < rd; i++ {
		// filename
		fn := fmt.Sprintf("mp-%d-%d", assign, i);
		fobj, _ := os.OpenFile(fn, os.O_RDWR | os.O_CREATE, 0644);

		// json encoder
		enc := json.NewEncoder(fobj);
		for _, kv := range intermediates[i] {
				err := enc.Encode(&kv);
				if err != nil {
					break;
				}
		}

		// finish
		fobj.Close();
	}
}

func RdExec(reply *MRReply, reducef func(string, []string) string) {
	// get needed fields
	nfile := reply.Nfile;
	assign := reply.Ird;

	// read all intermediate results.
	intermediate := []KeyValue{};
	for f := 0; f < nfile; f++ {
		fn := fmt.Sprintf("mp-%d-%d", f, assign);
		file, _ := os.Open(fn);
		dec := json.NewDecoder(file);
		for {
			var kv KeyValue;
			if err := dec.Decode(&kv); err != nil {
				break;
			}
			intermediate = append(intermediate, kv);
		}
		file.Close();
	}

	// by mrsequential.go, sort and "reduce" the results.
	sort.Sort(ByKey(intermediate))
	oname := fmt.Sprintf("mr-out-%d", assign);
	ofile, _ := os.Create(oname)

	// reduce the results.
	i := 0;
	for i < len(intermediate) {
		j := i + 1;
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++;
		}
		values := []string{};
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value);
		}
		output := reducef(intermediate[i].Key, values);

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output);

		i = j;
	}

	ofile.Close();
}

// real function to make an RPC call to the coordinator.
func CallReal(mrfn MrFunc, rp *MRReply) {
	// by previous executed task, fill in args.
	args := MRArgs{};
	args.Task = rp.Task;
	args.Id = rp.Id;
	switch rp.Task {
	case tmap: args.Imap = rp.Imap;
	case trd: args.Ird = rp.Ird;
	default:
	}

	// send the RPC request.
	*rp = MRReply{};
	ok := call("Coordinator.Real", &args, rp);
	if !ok {
		// nice crash
		fmt.Printf("call failed.\n");
		os.Exit(1);
	}

	switch rp.Task {
	case texit: os.Exit(0);
	case tmap: MapExec(rp, mrfn.mapf);
	case trd:  RdExec(rp, mrfn.reducef);
	case trt:  time.Sleep(time.Duration(1 * time.Second));
	}
}

//
// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
//
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
	}
}

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
