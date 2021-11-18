# Rust implementation of distributed key-value store which has REST interfaces

- TODO list (Japanese, includes finished tasks)
  - https://gist.github.com/ryogrid/9b1f340babfbb6cdc9d03e57b7834e71

## Distributed KVS
- Architecture (Japanese)
  - https://github.com/ryogrid/rust_dkvs/blob/master/ARCHITECTURE.md
- Referenced site (about REST API implementation. first one is Japanese) 
  - https://qiita.com/yukinarit/items/c5128e67d168b4f39983  
  - https://rocket.rs/v0.4/guide/getting-started/

- Fault torelance
  - data replication is implemented
  - functionality for keeping network healthy is implemented

- Data consistency
  - care for data consistency is not enough yet...
  - First, **running test under node downs and joins is planned** 

- How to execute node daemon
  - $ curl https://sh.rustup.rs -sSf | /bin/bash -s -- -y --default-toolchain nightly
  - $ rustup override set nightly
  - $ rustup update && cargo update
  - $ cargo run [born_id: 1...N] [IP addr to bind] [Port number to bind] [IP addr of medietor] [Port number of medietor] [log output path: currently not referenced]

- Setup KVS system
    - **launch example of node daemons which compose KVS system**
      - **you can build KVS system which is composed of multiple machines if you place rust_dkvs program binary to the machines and kick these with some tool like SSH**
        - procedures wrote below are single machine example for easy trying
      - **born_id of first node must be 1** but born_id of other node has no restriction except thst "1" can't be used
        - "1" can be used by first node only 
      - **IP address and port number has no restriction but all nodes should be able to communicate directly with each other**
    - $ cargo run 1 127.0.0.1 11000 127.0.0.1 10999 ./  
    - $ cargo run 2 127.0.0.1 11001 127.0.0.1 11000 ./
    - $ cargo run 3 127.0.0.1 11002 127.0.0.1 11000 ./
    - ....

- REST interfaces which are offered by KVS system
  - for easy testing with Web browser or something (use http GET request)
    - http://[node addr]:[node_port]/global_put_simple?key=[string only includes ascii character]&val=[string only includes ascii character]
    - http://[node addr]:[node_port]/global_get_simple?key=[string only includes ascii character]
    - http://[node addr]:[node_port]/global_delete_simple?key=[string only includes ascii character]
  - for using from code or HTTP client tool (use http POST request to send JSON text)
    - **charactor code of body part must be UTF-8**
    - http://[node addr]:[node_port]/global_put  
  　  - body at POST -> { "key_str" : "[charactors]", "val_str" : "[charactors]" }  
    - http://[node addr]:[node_port]/global_get  
  　  - body at POST -> "[key charactors]"  
    - http://[node addr]:[node_port]/global_delete  
      - body at POST -> "[key charactors]" 

- Utility CLI tool (tools/dkvs_client.go)
  - setup chord network (on local machine)
    - $ go run -op=setup-nodes -arg1=[launch nodes num]
  - KVS system network health check
    - $ go run -op=check-chain
    - if the network is helthy, launched or alive nodes (process) are listed without duplication
  - testing put values to KVS via specified node
    - $ go run -op=put-test-values -arg1="127.0.0.1:11000"
    - put 100 values
    - node you specify with arg1 has no restiction like that local addresses can be only specified. above is example
  - testing get already put values from KVS via specified node
    - $ go run -op=get-test-values -arg1="127.0.0.1:11005"
    - get 100 values 

## Simulator of distributed KVS (chord_sim dir)
- design verification with simulator wrote by **Python** (**verification is finished**)
  - you can simulate distributed kvs working  behavior. on the simulation put, get, stabilize, join operations are issued continuously under node down occuring condition.

## Simulator of distributed KVS (chord_sim_rust dir)
- design verification with simulator wrote by **Rust** (**verification is finished**)
- Rust implemantation of chord simulator can be executed like below 
  - $ curl https://sh.rustup.rs -sSf | /bin/bash -s -- -y --default-toolchain nightly
  - $ rustup override set nightly
  - $ rustup update && cargo update
  - $ cd chord_sim_rust
  - $ cargo run

## Runnable platforms for KVS system (= you can build the daemon binary for the platform)
- Windows native
  - dev env is also OK
- MacOS
  - dev env is also OK
- Linux (and Windows Subsystem for Linux environment)
  - dev env is also OK
  - probably ...
- other UNIX like OS environments
  - please try!
