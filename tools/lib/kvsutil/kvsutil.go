package kvsutil

import (
	"fmt"
	"os/exec"
	"strconv"
	"time"
	"tools/lib/gval"
)

func StartANode(born_id int, bind_addr string, bind_port int, tyukai_addr string, tyukai_port int, log_dir string) {
	shortcut_script := ""
	if gval.Platform == "windows" {
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

func SetupNodes(num int) {
	start_port := 11000
	cur_port := start_port
	for ii := 0; ii < num; ii++ {
		//start_a_node(ii+1, bind_ip_addr, cur_port+ii, bind_ip_addr, cur_port-1, "./")
		StartANode(ii+1, gval.BindIpAddr, cur_port+ii, gval.BindIpAddr, start_port, "./")
		//		cur_port++
		fmt.Printf("launched born_id=%d\n", ii+1)
		time.Sleep(time.Second * 5)
	}
}
