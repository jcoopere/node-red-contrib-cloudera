# node-red-contrib-cloudera
A collection of Node-RED nodes for integrating with Cloudera's software distribution including Apache Hadoop.

## Installation
To install these nodes, you can either install it locally within your user data directory (by default, $HOME/.node-red):

`cd $HOME/.node-red
npm install node-red-contrib-cloudera`
or globally alongside Node-RED:

`sudo npm install -g node-red-contrib-cloudera`

You will need to restart Node-RED for it to pick-up the new nodes.

## Node-RED Nodes
This package includes the following Node-RED nodes:
  - kafka producer
  - kafka consumer
  - avro

### kafka producer
This node publishes the content of `msg.payload` as an Apache Kafka message.

See this node's "info" tab for more information about configuration, usage, and behavior.

#### Prerequisites
Using this node requires that you have Apache Kafka running properly along with Apache ZooKeeper, and a Kafka topic created.

### kafka consumer
This node consumes Apache Kafka messages and outputs the message in `msg.payload`.

See this node's "info" tab for more information about configuration, usage, and behavior.

#### Prerequisites
Using this node requires that you have Apache Kafka running properly along with Apache ZooKeeper, a Kafka topic created, and messages being published to that topic.

#### Known Issues / Troubleshooting
  1. If you shut down Node-RED and quickly restart it you may see the error "FailedToRebalanceConsumerError: Exception: NODE_EXISTS[-110]" and the Kafka Consumer fail to initialize. This occurs when you attempt to create a Kafka Consumer with the same ID as a previously existing consumer before the ZooKeeper session for the previously existing consumer has timed out. Solutions to this can include stopping Node-RED and waiting for at least as long as the ZooKeeper session timeout, lowering the ZooKeeper session timeout, or providing a new, unique ID for the Kafka Consumer.

### avro
This node allows for JavaScript Objects (and JSON strings) to be serialized as Apache Avro records, and for Apache Avro records to be deserialized as JavaScript Objects.

See this node's "info" tab for more information about configuration, usage, and behavior.

#### Prerequisites
Using this node requires that you have a valid Avro Schema for your records, or are able to write one.
