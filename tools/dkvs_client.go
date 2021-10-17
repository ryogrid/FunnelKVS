package main

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"net/http"
)

func test_get_request_which_has_query_string() {
	url := "http://localhost:8000/get-param-test?param1=aaaaaa&param2=bbbbbb"
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

func main() {
	//test_get_request_which_has_query_string()
	test_post_request_deserialize()
	fmt.Println("finished!")
}
