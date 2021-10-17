package main

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"net/http"
	"os/exec"
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

// このプログラムがtoolsディレクトリ直下で実行されている前提
func test_process_exec() {
	err := exec.Command("../target/debug/rust_dkvs", "3", "5", "100501").Start()
	//out, err := exec.Command("../target/debug/rust_dkvs", "3", "5", "100501").Output()
	if err != nil {
		fmt.Println(err)
	}
	//fmt.Printf("%s\n", out)
}

func main() {
	// TODO: 必要になったら引数処理できるようにする https://qiita.com/nakaryooo/items/2d0befa2c1cf347800c3

	//test_get_request_which_has_query_string()
	//test_post_request_deserialize()
	test_process_exec()
	fmt.Println("finished!")
}
