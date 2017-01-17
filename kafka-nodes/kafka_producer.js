module.exports = function(RED) {
	var kafka = require('kafka-node');

	/*
	 * Kafka Producer Node
	 */
	function kafkaProducerNode(config) {
		RED.nodes.createNode(this, config);
		var node = this;

		this.kafkaZk = config.zk;
		this.kafkaTopics = config.topics.split(',');
		this.kafkaClientId = config.kafkaClientId ? config.kafkaClientId : null;
		this.zkSessionTimeout = config.zkSessionTiemout ? config.zkSessionTimeout : null;
		this.zkSpinDelay = config.zkSpinDelay ? config.zkSpinDelay : null;
		this.zkRetries = config.zkRetries ? config.zkRetries : null;
		this.kafkaNoAckBatchSize = config.noAckBatchSize ? config.noAckBatchSize : null;
		this.kafkaNoAckBatchAge = config.noAckBatchAge ? config.noAckBatchAge : null;
		this.kafkaRequireAcks = config.requireAcks ? config.requireAcks : null;
		this.kafkaAckTimeoutMs = config.ackTimeoutMs ? config.ackTimeoutMs : null;
		this.kafkaPartitionerType = config.partitionerType ? config.partitionerType : null;

		this.kafkaClientZkOptions = {
			sessionTimeout: this.zkSessionTimeout,
			spinDelay: this.zkSpinDelay,
			retries: this.zkRetries
		}

		this.kafkaClientNoAckBatchOptions = {
			noAckBatchSize: this.kafkaNoAckBatchSize,
			noAckBatchAge: this.kafkaNoAckBatchAge
		}

		/*
		// TODO: implement SSL options
		this.kafkaClientSslOptions = {
			// TODO: see https://nodejs.org/api/tls.html#tls_new_tls_tlssocket_socket_options for options related to SSL
		}
		*/


		// Trim whitespace from Kafka topic names (in case user put spaces after commas).
		this.kafkaTopics.forEach(function(tpc, i) {
			node.kafkaTopics[i] = tpc.trim();
		});

		var kafkaClient = new kafka.Client(this.kafkaZk, null, this.kafkaClientZkOptions, this.kafkaClientNoAckBatchOptions);
		// TODO: implement SSL options
		//var kafkaClient = new kafka.Client(this.kafkaZk, this.kafkaClientId, this.kafkaClientZkOptions, this.kafkaClientNoAckBatchOptions, this.kafkaClientSslOptions);

		var kafkaProducerOptions = {
			requireAcks: this.kafkaRequireAcks,
			ackTimeoutMs: this.kafkaAckTimeoutMs,
			partitionerType: this.kafkaPartitionerType
		}

		var kafkaProducer = new kafka.HighLevelProducer(kafkaClient, kafkaProducerOptions);

        this.status({fill: "green", shape: "dot", text: "connected to " + node.kafkaZk});

		try {
			this.on('input', function(msg) {
				var kafkaPayloads = [];
				node.kafkaTopics.forEach(function(tpc) {
					kafkaPayloads.push({topic: tpc, messages: msg.payload})
				});

				kafkaProducer.send(kafkaPayloads, function(err, data) {
					if (err) {
						node.error(err);
					}
				});
			});

			this.on('close', function(done) {
				kafkaClient.close(cb);
			});
		} catch (err) {
			node.error(err);
		}
	}
	RED.nodes.registerType("kafka producer", kafkaProducerNode);
}