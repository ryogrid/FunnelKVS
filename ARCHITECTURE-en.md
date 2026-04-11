# Design notes for a distributed KVS developed to learn Rust (Note: Some descriptions may differ from the final product)

- A data store that stores data on memory, such as Redis or Memcached

- Considering the need to cross NATs (i.e. when nodes exist in multiple different networks) by relaying data through other nodes, the implementation is expected to be quite complicated, so this time we will assume that all nodes are in a single network and that direct communication is possible between them

- Each node provides put, get, and delete as a REST API to access the configured KVS

- Since we are not placing importance on performance this time, we will limit the data that can be stored to those that can be interpreted as JSON format (the value part)

- We do not expect the key part to be very large, so we will include it in the URL when calling the API

- In addition to accessing the KVS, the communication for configuring the DHT will also be defined as a REST API (for the time being), and communication between nodes will be carried out using that (since it is troublesome to write socket programming every time). If it turns out that the implementation becomes complicated if the connection is not kept open continuously, we will review it.
- It will use Distributed Hash Table (DHT) technology and operate without a central server.
- It will use "Chord", the most popular DHT algorithm (protocol).
- Reference materials and websites
- [I tried implementing "Chord Protocol", a scalable naming algorithm for distributed systems - Qiita](https://qiita.com/taisho6339/items/7f849b65e2deab6759a1)
- [Introduction to DHT using the Chord algorithm - slideshare](https://www.slideshare.net/did2/chorddht)
- [VIOPS04: Basic knowledge of DHT - slideshare](https://www.slideshare.net/viopsjp/dht20091211)
- [Chord: A Scalable Peer-to-peer Lookup Protocol for Internet Applications - slideshare](https://www.slideshare.net/GertThijs/chord-presentation)
- How to use the Chord network
- The Chord network
- The Chord network is used only as a kind of name resolution mechanism to obtain the address of the "responsible node" from the ID of the data
- For all put, get, and delete operations, once the "responsible node" and its address are identified, the corresponding operation is requested directly to that node
- When building a wide-area distributed network, consideration must be given to handling nodes inside the NAT when creating routing and route tables, but in addition to that, it is thought that it will not work unless the proportion of nodes accessible by global IP in the entire network is increased by using UPnP so that the daemon of each node opens a hole in the NAT and acts to be accessible by global IP.
- Regarding implementation
- This is pseudocode written on page 9 of the VIOSP04 slide, but if the node space is larger than 6 bits, it seems that a recursive or loop-based call will be necessary somewhere
- As an example, the top A function called n.find_successor is executed in a while loop while updating n', and continues until it reaches a predecessor (n') whose successorList contains the ID targeted by n'.find_predecessor, and returns the information of the "responsible node" (and the information of the node in successorList located behind it) that it is looking for.

- If the network stabilization has not been completed locally, and if unfortunately it is not possible to find a node that holds the desired data, is it okay to return an error?

- If the node that should be in charge of the data ID, or all of the subsequent nodes, do not hold the data.

- If communication problems are not taken into account, it is currently unknown whether it is possible to roughly determine the worst-case time required for a search, taking into account the size of the network, the speed of the actual network below, and the processing time of each node, and whether it is always possible to determine that the data cannot be obtained within that time (if this is not the case, in other words, if there are cases where the node transitions in the search process become chaotic and the process never ends, it is necessary to add a timeout process).

- First, for simplicity, we will assume that there is only one predicessor and one successor.
- Assuming that data replication is performed separately, even if a new node joins, if there are about ln(total number of nodes) successors, it may be possible to get the data from the successors in order if the responsible node does not have it, without delegating the data to the new responsible node.
- If we are going to delegate, it seems like it would be good to periodically search for the responsible node and put the data that is not in charge of the data that you hold, but if there is a certain amount of data, you will need to think about the data structure of the data store that holds the data so that you can efficiently pass things to the same node all at once.
- For now, we will implement it under the assumption that nodes will not leave, but once we have something that works, we will also need to think about the sequence in case of node departure. Specifically, the main thing to think about is what to do if you cannot access a node that was in the routing table. All that's left is to add a data replication process.
- Stored data will be kept only in memory, and persistence to storage will not be considered for now.
- Data will not be persisted to storage, but data replication with neighboring nodes in the DHT address space will be implemented. This will mean that data will not be lost unless all or a large number of nodes are dropped at once (it should be).
- The implementation will be done using the Rust language, and will be done from scratch, except for the framework for REST API implementation and libraries that handle very general processing.
- In order to understand the Chord algorithm and verify the design based on it, we plan to create a simple Chord network simulator using Python (currently the first choice).
- This is the first time we've used the Rust language properly to write a real system, and if unexpected behavior occurs due to an error in the Chord implementation method, it is expected that the debugging costs will be very high.
