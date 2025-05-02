# Lab 4: Great Value Kafka

## Description of project topic, goals, and tasks

The goal was to create a distributed message queue much like Kafka, but the type of Kafka you'd find at walmart for cheap.

Features:
- Publish/subscribe
- Concurrent request handling
- Message TTL and evictions to save space (cron job to sweep the queue)
- Consumer groups
- Ordering guarantees by partition
- Key based partitioning for custom load balancing
- Multiple topics
- Hierarchical topics
- Many publishers, many subscribers, no limit aside from hardware
- Configurable partition size, max message size, and time to live

Overall Design:

![alt text](image.png)

#### Limitations:

- We were unable to integrate RAFT in time, so we support no fault tolerance or replication
- We support topic trees, but it is static; the topic leaf nodes should all be setup before subscribing. If you update the leaf nodes afterwards, you will still get the new items across all hierachies but if you add a new child later you won't get that

## Dependencies to run this code

- None! lets goooo

## Description of tests and how to run them

See here: https://docs.google.com/document/d/1PUz_Ap47RB0YX0I_Y0jd_4U8MbQTRrpURDu0zYklmCI/edit?tab=t.0

0. Setup (Init the codebase and test framework setup Google Tests, setup RAFT library)
1. Single publisher, single subscriber successful read, one partition
2. Single topic, multiple producers, multiple subscribers, one multiple partitions, concurrent request, high pressure
3. Check at least once delivery when subscribers nack or undelivered in any case, (the above does not apply actually) eviction policy should be satisfied, if there is no subscriber then the message should not persist (all read)
4. Replication works across brokers (RAFT integration), broker goes down but MQ intact
5. Partitions should work, check the ordering, messages with the same key should go into the same partition, load balancing across partitions as well (handled in 2)
6. Load balancing across broker
7. Multiple producers, multiple subscribers, multiple topics, later on hierarchical or overlapping topics test as well


- Docs: `make docs`
- Tests: `go test -C ./src/greatvaluekafka -run Final`
