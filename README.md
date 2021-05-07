# Rust implementation of distributed key-value store which has REST interfaces

## Distributed KVS (**almost not implemented yet...**)
- Architecture (Japanese)
  - https://github.com/ryogrid/rust_dkvs/blob/master/ARCHITECTURE.md
- Referenced site (about REST API implementation. first one is Japanese) 
  - https://qiita.com/yukinarit/items/c5128e67d168b4f39983  
  - https://rocket.rs/v0.4/guide/getting-started/

- Program execution needed to use Rocket at project directory
  - $ rustup override set nightly
  - $ rustup update && cargo update
  
## Simulator of distributed KVS (chord_sim dir)
- design verification with simulator wrote by **Python** (**verification is finished**)
  - you can simulate distributed kvs working  behavior. on the simulation put, get, stabilize, join operations are issued continuously under node down occuring condition.
- TODO list (Japanese, includes finished tasks)
  - https://gist.github.com/ryogrid/9b1f340babfbb6cdc9d03e57b7834e71

## Simulator of distributed KVS (chord_sim_rust dir)
- design verification with simulator wrote by **Rust** (**I'm working on porting from python implementation now**)
- Rust implemantation of chord simulator can be executed like below 
  - $ curl https://sh.rustup.rs -sSf | /bin/bash -s -- -y --default-toolchain nightly
  - $ rustup override set nightly
  - $ rustup update && cargo update
  - $ cd chord_sim_rust
  - $ cargo run

## Runnable platforms (= Rust and Python usable platforms)
- Linux (**Windows Subsystem for Linux** environment is also OK)
- MacOS (Maybe)
- Windows native (I have not tried yet...)
- other Unix like OS environments (please try!)
