module.exports = function(RED) {
	var kafka = require('kafka-node');

	/*
	 * Kafka Producer Node
	 */
	function kafkaProducerNode(config) {
		RED.nodes.createNode(this, config);

		this.zk = config.zk;
		this.topics = config.topics;
		this.isAvro = config.isAvro; // currently this does nothing TODO decide what it should do, or remove
		this.debug = config.debug;
		// TODO: include other client options listed on https://www.npmjs.com/package/kafka-node
		this.clientId = 'kafka-client-node'; // should this be an arg?
		// this.zkOptions
		// this.noAckBatchOptions
		// this.sslOptions

		var kafkaClient = new kafka.Client(this.zk, this.clientId);

		// TODO: make these configurable
		var kafkaProducerOptions = {
			requireAcks: 1,
			ackTimeoutMs: 100,
			partitionerType: 2
		}

		var kafkaProducer = new kafka.Producer(kafkaClient, kafkaProducerOptions);
        this.status({fill:"green", shape:"dot", text:"connected to "+ this.zk});

		var node = this;

		try {
			this.on('input', function(msg) {
				// TODO: payloads should start as an empty array and be pushed to for each target kafka topic
				// var payloads = []
				// for (topic : topics) payloads.push
				// TODO: review this...
				//kafkaPayloads = [];
				//kafkaPayloads.push(msg.payload);
				var kafkaPayloads = [{topic: node.topics, messages: msg.payload}];

				kafkaProducer.send(kafkaPayloads, function(err, data) {
					if (err) {
						node.error(err);
					}
					if (node.debug) node.log("Message published to Kafka.\n\tZK: " + node.zk + "\n\tTopic: " + node.topics + "\n\tMessages: " + kafkaPayloads.messages);
				});
			});

			this.on('close', function(done) {
				kafkaClient.close(cb);
			});
		} catch (e) {
			node.error(e);
		}
	}
	RED.nodes.registerType("kafka producer", kafkaProducerNode);
}