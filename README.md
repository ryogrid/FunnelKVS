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
  - implementation for keeping network helth exists
  - not perfect yet...

- Data coherence
  - care of data coherence is not strong yet...
  - **running test under node downs and joins is pranned** 

- this can be executed like below
  - $ curl https://sh.rustup.rs -sSf | /bin/bash -s -- -y --default-toolchain nightly
  - $ rustup override set nightly
  - $ rustup update && cargo update
  - $ cargo run [born_id: 1...N] [IP addr to bind] [Port number to bind] [IP addr of medietor] [Port number of medietor] [log output path: currently not referenced]
    - **node launch example is below**
      - **born_id of first node must be 1** but born id of other node is free except for 1
    - $ cargo run 1 127.0.0.1 11000 127.0.0.1 10999 ./  
    - $ cargo run 2 127.0.0.1 11001 127.0.0.1 11000 ./
    - $ cargo run 3 127.0.0.1 11002 127.0.0.1 11000 ./
    - ....

- REST interfaces
  - for easy testing with browser or something (use http GET request)
    - http://[node addr]:[node_port]/global_put_simple?key=[string only includes ascii character]&val=[string only includes ascii character]
    - http://[node addr]:[node_port]/global_get_simple?key=[string only includes ascii character]
    - http://[node addr]:[node_port]/global_delete_simple?key=[string only includes ascii character]
  - for using from some program (use http POST request to send JSON text)   
    - http://[node addr]:[node_port]/global_put  
  　  - body at POST -> { "key_str" : [utf8 string], "val_str" : [ut8 string] }  
    - http://[node addr]:[node_port]/global_put  
  　  - body at POST -> { "key_str" : [utf8 string] }  
    - http://[node addr]:[node_port]/global_delete  
      - body at POST -> { "key_str" : [utf8 string] }  
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

## Runnable platforms (= Rust and Python usable platforms)
- Linux (**Windows Subsystem for Linux** environment is also OK)
- Windows native
- MacOS (Maybe)
- other Unix like OS environments (please try!)
