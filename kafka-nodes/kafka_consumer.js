module.exports = function(RED) {
	var kafka = require('kafka-node');

	/*
	 * Kafka Consumer Node
	 */
	function kafkaConsumerNode(config) {
		RED.nodes.createNode(this, config);

		this.kafkaZk = config.zk;
		this.kafkaTopics = config.topics.split(',');
		this.kafkaClientId = config.kafkaClientId ? config.kafkaClientId : null;
		this.zkSessionTimeout = config.zkSessionTiemout ? config.zkSessionTimeout : null;
		this.zkSpinDelay = config.zkSpinDelay ? config.zkSpinDelay : null;
		this.zkRetries = config.zkRetries ? config.zkRetries : null;
		this.kafkaNoAckBatchSize = config.noAckBatchSize ? config.noAckBatchSize : null;
		this.kafkaNoAckBatchAge = config.noAckBatchAge ? config.noAckBatchAge : null;
		this.kafkaConsumerGroupId = config.consumerGroupId ? config.consumerGroupId : null;
		this.kafkaConsumerId = config.consumerId ? config.consumerId : null;
		this.kafkaAutoCommit = config.autoCommit ? config.autoCommit : true;
		this.kafkaAutoCommitInterval = config.autoCommitInterval ? config.autoCommitInterval : 5000;
		this.kafkaFetchMaxWaitMs = config.fetchWaitMaxMs ? config.fetchWaitMaxMs : 100;
		this.kafkaFetchMinBytes = config.fetchMinBytes ? config.fetchMinBytes : 1;
		this.kafkaFetchMaxBytes = config.fetchMaxBytes ? config.fetchMaxBytes : 1024 * 1024;
		this.kafkaFromOffset = config.fromOffset ? config.fromOffset : false;
		this.kafkaEncoding = config.kafkaEncoding;

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

		var node = this;

		// Trim whitespace from Kafka topic names (in case user put spaces after commas).
		this.kafkaTopics.forEach(function(tpc, i) {
			node.kafkaTopics[i] = tpc.trim();
		});

		var kafkaPayloads = [];
		this.kafkaTopics.forEach(function(tpc) {
			kafkaPayloads.push({topic: tpc.trim()});
		});

		var kafkaConsumerOptions = {
			groupId: this.kafkaConsumerGroupId,
			id: this.kafkaConsumerId,
			autoCommit: this.kafkaAutoCommit,
			autoCommitInterval: this.kafkaAutoCommitInterval,
			fetchMaxWaitMs: this.kafkaFetchMaxWaitMs,
			fetchMinBytes: this.kafkaFetchMinBytes,
			fetchMaxBytes: this.kafkaFetchMaxBytes,
			fromOffset: this.kafkaFromOffset,
			encoding: this.kafkaEncoding
		}

		try {
			var kafkaClient = new kafka.Client(this.kafkaZk, this.kafkaClientId, this.kafkaClientZkOptions, this.kafkaClientNoAckBatchOptions);
			// TODO: implement SSL options
			//var kafkaClient = new kafka.Client(this.kafkaZk, this.kafkaClientId, this.kafkaClientZkOptions, this.kafkaClientNoAckBatchOptions, this.kafkaClientSslOptions);

			var kafkaConsumer = new kafka.HighLevelConsumer(kafkaClient, kafkaPayloads, kafkaConsumerOptions);

	        node.status({fill: "green", shape: "dot", text: "connected to " + this.kafkaZk});

			kafkaConsumer.on('message', function(msg) {
				node.send({payload: msg});
			});

			kafkaConsumer.on('error', function(err) {
				node.error(err);
			});

			this.on('close', function(done) {
				kafkaConsumer.close(cb);
				kafkaClient.close(cb);
			});
		} catch (err) {
			node.error(err);
		}
	}
	RED.nodes.registerType("kafka consumer", kafkaConsumerNode);
}