package grpcver

import (
	//"context"
	"fmt"

	"strconv"
	"tools/lib/gval"
	"tools/lib/rest"
	"tools/lib/rustdkvs"

	"golang.org/x/net/context"
	"google.golang.org/grpc"
)

func GrpcGetNodeInfo() {
	var conn *grpc.ClientConn
	conn, err := grpc.Dial(":9000", grpc.WithInsecure())
	if err != nil {
		fmt.Printf("did not connect: %s\n", err)
	}
	defer conn.Close()

	c := rustdkvs.NewRustDKVSClient(conn)

	response, err := c.GrpcGetNodeInfo(context.Background(), &rustdkvs.VOID{Val: 0})
	if err != nil {
		fmt.Printf("Error when calling grpc_get_node_info: %s\n", err)
	}
	fmt.Println(response)
}

func CheckChainWithSuccessorInfo() {
	const endpoint_path = "/get_node_info"
	start_port := 11000
	start_addr := gval.BindIpAddr + ":" + strconv.Itoa(start_port)

	succ_addr := start_addr
	cur_addr := ""
	born_id := -1.0
	node_id := -1.0
	counter := 0
	request_count := 0
	is_success_reqest := false
	for {
		resp_json, err := rest.HttpGetRequest(succ_addr, endpoint_path)
		request_count++
		if request_count == rest.CcheckNodeLimit {
			fmt.Println("Error: travarse times may exceeded launched nodes!")
			break
		}
		if err != nil {
			if !is_success_reqest {
				start_port += 1
				succ_addr = gval.BindIpAddr + ":" + strconv.Itoa(start_port)
				continue
			} else {
				fmt.Println("Error: successor should downed and information of successor is not recovered.")
				break
			}
		}
		is_success_reqest = true
		cur_addr, born_id, node_id, succ_addr = rest.ExtractAddrAndBornId(resp_json)
		counter++
		fmt.Printf("addr=%s born_id=%f node_id_ratio=%f counter=%d succ_addr=%s\n", cur_addr, born_id, (node_id/0xFFFFFFFF)*100.0, counter, succ_addr)
		if succ_addr == start_addr {
			break
		}
	}
}
