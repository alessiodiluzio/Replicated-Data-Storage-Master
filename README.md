# Replicated Data Storage

The purpose of the project contained in this repository is the development 
of replicated data storage, the deployment of which was performed on Amazon EC2 instances.

## System architecture

The Replicated Data Storage system consists of three types of nodes:

* Master: Manages the mapping between a file name and location of replicas. It is responsible for coordinating the other nodes in the system by taking care of allocating and de-allocating instances elastically to respond to changes in the load on the system.
* DataNode: Stores files and their contents.
* Cloudlet: peripheral node of the system, designed to be located at the edges of the network in the vicinity of data sources (Device Clients, Sensors...).It is the front end of the system being the interface node with the system for supported operations.

The system has a multi-Master architecture in which each individual Master knows and can contact all the others and manages a subset of DataNodes and Cloudlets.
The number of Masters, DataNodes, and Cloudlets in the system at startup is fully configurable.

This repository contains the code of the Master and DataNodes


### Master

The Master receives periodic heartbeat consequently, it can decide to allocate new nodes or remove nodes to balance monitored resources (CPU and RAM). The crash of a DataNode or CloudLet node is also detected, and it triggers a recovery procedure including a recovery phase of any lost data.
The Master provides load balancing by moving the most accessed or largest files from overloaded DataNodes to other less-used ones or in their absence to new nodes. 

The Master manages a mapping table between a file name and a replica location if it is on a DataNode it manages; it can still retrieve information about file placement by communicating with other Masters.

### Data Node

The contents of the files are stored in the DataNode instances, updates occur only through append operations, and are propagated from one replica to the next in a non-blocking and asynchronous manner with respect to the write operation.


## Implementation details 

The System is deployed on AWS EC2 instances.
The System is implemented in the Java language, communication between nodes in the system is done by exploiting Java RMI technology, and dependency management is done by the Maven tool.
RMI communication is done using the public IP addresses of EC2 instances.
The local tables in each node are stored in Apache Derby relational databases embedded in memory.



