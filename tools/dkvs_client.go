package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
	"net/http"
	"os/exec"
	"runtime"
	"strconv"
	"time"
)

func test_get_request_which_has_query_string() {
	url := "http://localhost:8000/get-param-test?param1=aaaaaa&param2=bbbbbb"
	// TODO: クエリストリングでパラメータを渡す際にURIエンコードが行われるか確認して
	//       されないようであればされるようにする（方法を確認しておく）必要あり
	req, _ := http.NewRequest("GET", url, nil)

	client := new(http.Client)
	resp, _ := client.Do(req)
	defer resp.Body.Close()

	byteArray, _ := ioutil.ReadAll(resp.Body)
	fmt.Println(string(byteArray))
}

func test_post_request_deserialize() error {
	url := "http://localhost:8000/deserialize"
	jsonStr := `{"node_id":100,"address_str":"kanbayashi","born_id":77,"successor_info_list":[{"node_id":100,"address_str":"kanbayashi","born_id":77,"successor_info_list":[],"predecessor_info":[],"finger_table":[null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null]}],"predecessor_info":[{"node_id":100,"address_str":"kanbayashi","born_id":77,"successor_info_list":[{"node_id":100,"address_str":"kanbayashi","born_id":77,"successor_info_list":[],"predecessor_info":[],"finger_table":[null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null]}],"predecessor_info":[],"finger_table":[null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null]}],"finger_table":[null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null]}`

	req, err := http.NewRequest(
		"POST",
		url,
		bytes.NewBuffer([]byte(jsonStr)),
	)
	if err != nil {
		return err
	}

	// Content-Type 設定
	req.Header.Set("Content-Type", "application/json")

	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	byteArray, _ := ioutil.ReadAll(resp.Body)

	fmt.Println(string(byteArray))

	return err
}

func test_get_request_Result_type_return() {
	url := "http://127.0.0.1:8000/result-type"
	// TODO: クエリストリングでパラメータを渡す際にURIエンコードが行われるか確認して
	//       されないようであればされるようにする（方法を確認しておく）必要あり
	req, _ := http.NewRequest("GET", url, nil)

	client := new(http.Client)
	resp, _ := client.Do(req)
	defer resp.Body.Close()

	byteArray, _ := ioutil.ReadAll(resp.Body)

	fmt.Println(string(byteArray))
}

// このプログラムがtoolsディレクトリ直下で実行されている前提
func test_process_exec() {
	err := exec.Command("../target/debug/rust_dkvs", "3", "5", "100501").Start()
	//out, err := exec.Command("../target/debug/rust_dkvs", "3", "5", "100501").Output()
	if err != nil {
		fmt.Println(err)
	}
	//fmt.Printf("%s\n", out)
}

func http_get_request(addr_and_port string, path_str string) (map[string]interface{}, error) {
	url := "http://" + addr_and_port + path_str
	// TODO: クエリストリングでパラメータを渡す際にURIエンコードが行われるか確認して
	//       されないようであればされるようにする（方法を確認しておく）必要あり
	req, _ := http.NewRequest("GET", url, nil)

	client := new(http.Client)
	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	byteArray, _ := ioutil.ReadAll(resp.Body)

	// JSONデコード
	var decoded_data interface{}
	if err := json.Unmarshal(byteArray, &decoded_data); err != nil {
		fmt.Println(err)
	}

	return decoded_data.(map[string]interface{}), err
}

func extract_addr_and_born_id(input_json map[string]interface{}) (string, float64, float64, string) {
	ret_addr := input_json["address_str"].(string)
	ret_born_id := input_json["born_id"].(float64)
	ret_node_id := input_json["node_id"].(float64)
	succ_list := input_json["successor_info_list"].([]interface{})
	succ_entry_0 := succ_list[0].(map[string]interface{})
	ret_succ_addr := succ_entry_0["address_str"].(string)
	return ret_addr, ret_born_id, ret_node_id, ret_succ_addr
}

//const bind_ip_addr = "192.168.3.13"
const bind_ip_addr = "127.0.0.1"

//const bind_ip_addr = "[::1]"
//const bind_ip_addr = "0.0.0.0"
const check_node_limit = 150

var platform string

func check_chain_with_successor_info() {
	const endpoint_path = "/get_node_info"
	start_port := 11000
	start_addr := bind_ip_addr + ":" + strconv.Itoa(start_port)

	succ_addr := start_addr
	cur_addr := ""
	born_id := -1.0
	node_id := -1.0
	counter := 0
	request_count := 0
	is_success_reqest := false
	for true {
		resp_json, err := http_get_request(succ_addr, endpoint_path)
		request_count++
		if request_count == check_node_limit {
			fmt.Println("Error: travarse times may exceeded launched nodes!")
			break
		}
		if err != nil {
			if is_success_reqest == false {
				start_port += 1
				succ_addr = bind_ip_addr + ":" + strconv.Itoa(start_port)
				continue
			} else {
				fmt.Println("Error: successor should downed and information of successor is not recovered.")
				break
			}
		}
		is_success_reqest = true
		cur_addr, born_id, node_id, succ_addr = extract_addr_and_born_id(resp_json)
		counter++
		fmt.Printf("addr=%s born_id=%f node_id_ratio=%f counter=%d succ_addr=%s\n", cur_addr, born_id, (node_id/0xFFFFFFFF)*100.0, counter, succ_addr)
		if succ_addr == start_addr {
			break
		}
	}
}

func start_a_node(born_id int, bind_addr string, bind_port int, tyukai_addr string, tyukai_port int, log_dir string) {
	shortcut_script := ""
	if platform == "windows" {
		shortcut_script = "rust_dkvs.bat"
	} else {
		shortcut_script = "./rust_dkvs.sh"
	}

	err := exec.Command(
		shortcut_script, //"rust_dkvs.bat", //"../target/debug/rust_dkvs",
		strconv.Itoa(born_id),
		bind_addr,
		strconv.Itoa(bind_port),
		tyukai_addr,
		strconv.Itoa(tyukai_port),
		log_dir).Start()
	if err != nil {
		fmt.Println(err)
	}
}

func setup_nodes(num int) {
	start_port := 11000
	cur_port := start_port
	for ii := 0; ii < num; ii++ {
		//start_a_node(ii+1, bind_ip_addr, cur_port+ii, bind_ip_addr, cur_port-1, "./")
		start_a_node(ii+1, bind_ip_addr, cur_port+ii, bind_ip_addr, start_port, "./")
		//		cur_port++
		fmt.Printf("launched born_id=%d\n", ii+1)
		time.Sleep(time.Second * 5)
	}
}

func global_put_simple(addr_and_port string, key string, val string) (map[string]interface{}, error) {
	return http_get_request(addr_and_port, "/global_put_simple?key="+key+"&val="+val)
}

func global_get_simple(addr_and_port string, key string) (map[string]interface{}, error) {
	return http_get_request(addr_and_port, "/global_get_simple?key="+key)
}

// 固定されたテスト用の keyとvalueの組み合わせを global_putする
func put_test_values(addr_and_port string) {
	for ii := 0; ii < 100; ii++ {
		key := strconv.Itoa(ii)
		val := key
		fmt.Printf("put request key=%s\n", key)
		_, err := global_put_simple(addr_and_port, key, val)
		if err != nil {
			fmt.Println("global_put_simple request failed:" + err.Error())
		}
	}
}

func get_test_values(addr_and_port string) {
	num := 100
	start_unix_time := time.Now().Unix()
	for ii := 0; ii < num; ii++ {
		key := strconv.Itoa(ii)
		fmt.Printf("get request key=%s\n", key)
		resp_json, err := global_get_simple(addr_and_port, key)
		fmt.Println(resp_json)
		if err != nil {
			fmt.Printf("get missed key=%s\n", key)
		}
	}
	end_unitx_time := time.Now().Unix()
	time_to_get := float64(end_unitx_time-start_unix_time) / float64(num)
	fmt.Printf("%f sec/data\n", time_to_get)
}

func profile_get_node_info_throughput() {
	num := 50
	start_unix_time := time.Now().UnixNano()
	const endpoint_path = "/get_node_info"
	target_port := 11000
	target_addr := bind_ip_addr + ":" + strconv.Itoa(target_port)
	for ii := 0; ii < num; ii++ {
		_, err := http_get_request(target_addr, endpoint_path)
		//fmt.Println(resp_json)
		if err != nil {
			fmt.Printf("error:%s\n", err)
		}
	}
	end_unitx_time := time.Now().UnixNano()
	time_to_query := (float64(end_unitx_time-start_unix_time) / float64(num)) / float64(1000)
	fmt.Printf("%f usec/query\n", time_to_query)
}

func main() {
	platform = runtime.GOOS

	op := flag.String("op", "setup-nodes", "setup chord network")
	arg1 := flag.String("arg1", "30", "argument if operation needs it")
	flag.Parse()

	switch *op {
	case "setup-nodes":
		node_num, _ := strconv.Atoi(*arg1)
		setup_nodes(node_num)
		break
	case "check-chain":
		check_chain_with_successor_info()
		break
	case "put-test-values":
		addr_and_port := *arg1
		put_test_values(addr_and_port)
		break
	case "get-test-values":
		addr_and_port := *arg1
		get_test_values(addr_and_port)
		break
	case "profile-get-node-info":
		profile_get_node_info_throughput()
		break
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
