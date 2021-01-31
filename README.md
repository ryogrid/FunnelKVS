# Rust implementation of distributed key-value store which has REST interfaces

## Rust implementation (**almost not implemented yet**)
- Architecture (Japanese)
  - https://github.com/ryogrid/rust_dkvs/blob/master/ARCHITECTURE.md
- Referenced site (about REST API implementation. first one is Japanese) 
  - https://qiita.com/yukinarit/items/c5128e67d168b4f39983  
  - https://rocket.rs/v0.4/guide/getting-started/

- Commands execution needed to use Rocket at project directory
  - $ rustup override set nightly
  - $ rustup update && cargo update

## distributed kvs simulator (chord_sim dir)
- design verification with simulator wrote by python
  - you can simulate distributed kvs working  behavior. on the simulation put, get, stabilize, join operations are issued continuously under node down occuring condition.
  - **simulation code and design description as code on node down occuring condition are work in progress**
- TODO list (Japanese, includes finished tasks)
  - https://gist.github.com/ryogrid/9b1f340babfbb6cdc9d03e57b7834e71
