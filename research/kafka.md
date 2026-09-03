## Introduction to Apache Kafka

Apache Kafka is a message streaming tool that you can send a message or data to a consumer as a producer.
There are some components of Kafka following:
1. Broker - A part of Kafka cluster. Brokers are responsible for store, receive, and send the message(data) to consumers as as producer. In normal, the cluster continues to work and send the data even if one broker is failed. But there are some replication settings that they prevents the interruption of cluster's working:
    1. replication_factor > 3. At least 3 brokers should be copied.
    2. min.insync.replicas should be assigned accurately(in normal 2). Imagine that replication_factor: 3, but min.insync.replicas : 1 => so only 1 broker will be synchronic, rest of the brokers(2 brokers) will be failed, only 1  broker will work. But it will be cause of overload, and the risk  of the loss of the data.
    3. "acks" parameter
    acks=0 --> There is no broker is waited for being verified. If a broker is failed, all the data will be lost.
    acks=1 --> Only 1 broker - leader broker is waited for being verified. If only leader broker accepts all the data, but suddenly it is failed, all the data will be lost
    acks=all --> all the in-sync replics should give verification. That is the best and safest way, even if it works slowly.
    So acks=all is required.
    4. Imagine that leader broker is failed, but all the followers are not synchronized yet. If unclean.leader.election.enable=true, then Kafka will select asynchronic replica as a new leader -so it will be cause of the loss of the new messages. So unclean.leader.election.enable should be equal to false. 
    5. All the brokers should be separated physically. In contrast, while all brokers are failed at the same time, cluster will be stopped. 
2. Topic - A broker consists of several topics. They group and organize the messages. Every message is published in a special topic. Topic consists of some partitions.
3. Partition - All the messages are stored in partitions and every message has ascendly unique number inside the partition : the messages are ordered.
4. Offset - Consumer can see that to where the consumer read the messages.
5. Commit - After consumer read the messages, consumer commit the offset that "I 've read until it". (it is written to the topic named "__consumer_offset"). It is useful for consumers that they can know from where they can continue. You can commit automatically(with a defined interval) or manually.
6. Idempotency - even if there is a network issue, the message is sent twice, broker will accept it once: every message has unique sequence number --> producer ID, so the broker will recognize the duplicates. For idempotency: enabel.idempotency=true.
7. Consumer - a client who accept the message/data.
8. Producer - a user who write and send the messages/data to consumers.
9. Replication - every partition has several replicas and they stored in different brokers(leader and followers), so data will not be lost.
10. Consumer log - it shows the difference between producer's last offset and offset consumer read. It shows that how long does system fall behind.


