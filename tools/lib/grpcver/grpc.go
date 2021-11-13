package grpcver

import (
	//"context"
	"fmt"
	"strconv"
	"time"
	"tools/lib/gval"
	"tools/lib/rest"
	"tools/lib/rustdkvs"

	"golang.org/x/net/context"
	"google.golang.org/grpc"
)

func GrpcGetNodeInfo(address_port string) (*rustdkvs.NodeInfo, error) {
	var conn *grpc.ClientConn
	conn, err := grpc.Dial(address_port, grpc.WithInsecure())
	if err != nil {
		fmt.Printf("did not connect: %s\n", err)
	}
	defer conn.Close()

	c := rustdkvs.NewRustDKVSClient(conn)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	response, err := c.GrpcGetNodeInfo(ctx, &rustdkvs.VOID{Val: 0})
	//response, err := c.GrpcGetNodeInfo(context.Background(), &rustdkvs.VOID{Val: 0})
	return response, err
	// if err != nil {
	// 	fmt.Printf("Error when calling grpc_get_node_info: %s\n", err)
	// }
	// fmt.Println(response)
}

func ExtractAddrAndBornId(node_info *rustdkvs.NodeInfo) (string, int32, uint32, string) {
	ret_addr := node_info.AddressStr
	ret_born_id := node_info.BornId
	ret_node_id := node_info.NodeId
	succ_list := node_info.SuccessorInfoList
	succ_entry_0 := succ_list[0]
	ret_succ_addr := succ_entry_0.AddressStr
	return ret_addr, ret_born_id, ret_node_id, ret_succ_addr
}

func CheckChainWithSuccessorInfo() {
	//const endpoint_path = "/get_node_info"
	start_port := 11000
	start_addr := gval.BindIpAddr + ":" + strconv.Itoa(start_port)

	succ_addr := start_addr
	cur_addr := ""
	var born_id int32 = -1
	var node_id uint32 = 1
	counter := 0
	request_count := 0
	is_success_request := false
	is_fined := false
fin:
	for !is_fined {
		var node_info *rustdkvs.NodeInfo
		var err error
		var retry_count = 0
		for {
			node_info, err = GrpcGetNodeInfo(succ_addr)
			request_count++
			if request_count == rest.CcheckNodeLimit {
				fmt.Println("Error: travarse times may exceeded launched nodes!")
				is_fined = true
				break fin
			}
			if err != nil {
				if !is_success_request {
					start_port += 1
					succ_addr = gval.BindIpAddr + ":" + strconv.Itoa(start_port)
					break
				} else if retry_count < 3 {
					//同じアドレスでもう一回
					continue
				} else {
					fmt.Println("Error: successor should downed and information of successor is not recovered.")
					is_fined = true
					break fin
				}
			}
			is_success_request = true
			break
		}
		if is_success_request {
			cur_addr, born_id, node_id, succ_addr = ExtractAddrAndBornId(node_info)
			counter++
			fmt.Printf("addr=%s born_id=%d node_id_ratio=%f counter=%d succ_addr=%s\n", cur_addr, born_id, (float64(node_id)/float64(0xFFFFFFFF))*100.0, counter, succ_addr)
			if succ_addr == start_addr {
				break
			}
		}
	}
}
