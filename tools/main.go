package main

import (
	"flag"
	"fmt"
	"runtime"
	"strconv"
	"tools/gval"
	"tools/kvsutil"
	"tools/rest"
)

func main() {
	gval.Platform = runtime.GOOS

	op := flag.String("op", "setup-nodes", "setup chord network")
	arg1 := flag.String("arg1", "30", "argument if operation needs it")
	flag.Parse()

	switch *op {
	case "setup-nodes":
		node_num, _ := strconv.Atoi(*arg1)
		kvsutil.SetupNodes(node_num)
	case "check-chain":
		rest.CheckChainWithSuccessorInfo()
	case "put-test-values":
		addr_and_port := *arg1
		rest.PutTestValue(addr_and_port)
	case "get-test-values":
		addr_and_port := *arg1
		rest.GetTestValues(addr_and_port)
	case "profile-get-node-info":
		rest.ProfileGetNodeInfoThroughput()
	default:
		fmt.Println("dkvs_client -op=<operation-name> -arg1=<argument if needed>")
	}

	//test_get_request_which_has_query_string()
	//test_post_request_deserialize()
	//test_process_exec()
	//test_get_request_Result_type_return()
	//setup_nodes(40)
	//check_chain_with_successor_info()
	fmt.Println("finished!")
}
