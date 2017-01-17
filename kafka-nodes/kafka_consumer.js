module.exports = function(RED) {
	var kafka = require('kafka-node');

	/*
	 * Kafka Consumer Node
	 */
	function kafkaConsumerNode(config) {
		RED.nodes.createNode(this, config);
		var node = this;

		this.kafkaZk = config.zk;
		this.kafkaTopics = config.topics.split(',');
		this.kafkaClientId = config.kafkaClientId ? config.kafkaClientId : null;
		/*this.zkSessionTimeout = config.zkSessionTiemout ? config.zkSessionTimeout : null;
		this.zkSpinDelay = config.zkSpinDelay ? config.zkSpinDelay : null;
		this.zkRetries = config.zkRetries ? config.zkRetries : null;
		this.kafkaNoAckBatchSize = config.noAckBatchSize ? config.noAckBatchSize : null;
		this.kafkaNoAckBatchAge = config.noAckBatchAge ? config.noAckBatchAge : null;
		*/
		/*this.kafkaConsumerGroupId = config.consumerGroupId ? config.consumerGroupId : null;
		this.kafkaConsumerId = config.consumerId ? config.consumerId : null;
		this.kafkaAutoCommit = config.autoCommit ? config.autoCommit : true;
		this.kafkaAutoCommitInterval = config.autoCommitInterval ? config.autoCommitInterval : 5000;
		this.kafkaFetchMaxWaitMs = config.fetchWaitMaxMs ? config.fetchWaitMaxMs : 100;
		this.kafkaFetchMinBytes = config.fetchMinBytes ? config.fetchMinBytes : 1;
		this.kafkaFetchMaxBytes = config.fetchMaxBytes ? config.fetchMaxBytes : 1024 * 1024;
		this.kafkaFromOffset = config.fromOffset ? config.fromOffset : false;
		this.kafkaEncoding = config.kafkaEncoding;
		*/

		/*
		this.kafkaClientZkOptions = {
			sessionTimeout: this.zkSessionTimeout,
			spinDelay: this.zkSpinDelay,
			retries: this.zkRetries
		}
		*/
		this.kafkaClientZkOptions = {};
		if (config.zkSessionTimeout) this.kafkaClientZkOptions.sessionTimeout = config.zkSessionTimeout;
		if (config.zkSpinDelay) this.kafkaClientZkOptions.spinDelay = config.zkSpinDelay;
		if (config.zkRetries) this.kafkaClientZkOptions.retries = config.zkRetries;

		/*
		this.kafkaClientNoAckBatchOptions = {
			noAckBatchSize: this.kafkaNoAckBatchSize,
			noAckBatchAge: this.kafkaNoAckBatchAge
		}
		*/
		this.kafkaClientNoAckBatchOptions = {};
		if (config.kafkaNoAckBatchSize) this.kafkaClientNoAckBatchOptions.noAckBatchSize = config.kafkaNoAckBatchSize;
		if (config.kafkaNoAckBatchAge) this.kafkaClientNoAckBatchOptions.noAckBatchAge = config.kafkaNoAckBatchAge;

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

		var kafkaPayloads = [];
		this.kafkaTopics.forEach(function(tpc) {
			kafkaPayloads.push({topic: tpc});
		});

		/*var kafkaConsumerOptions = {
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
		*/
		this.kafkaConsumerOptions = {};
		if (config.consumerGroupId) this.kafkaConsumerOptions.groupId = config.consumerGroupId;
		if (config.consumerId) this.kafkaConsumerOptions.id = config.consumerId;
		if (config.autoCommit) this.kafkaConsumerOptions.autoCommit = config.autoCommit;
		if (config.autoCommitInterval) this.kafkaConsumerOptions.autoCommitInterval = config.autoCommitInterval;
		if (config.fetchWaitMaxMs) this.kafkaConsumerOptions.fetchMaxWaitMs = config.fetchWaitMaxMs;
		if (config.fetchMinBytes) this.kafkaConsumerOptions.fetchMinBytes = config.fetchMinBytes;
		if (config.fetchMaxBytes) this.kafkaConsumerOptions.fetchMaxBytes = config.fetchMaxBytes;
		if (config.fromOffset) this.kafkaConsumerOptions.fromOffset = config.fromOffset;
		if (config.kafkaEncoding) this.kafkaConsumerOptions.encoding = config.kafkaEncoding;

		try {
			var kafkaClient = new kafka.Client(node.kafkaZk, node.kafkaClientId, node.kafkaClientZkOptions, node.kafkaClientNoAckBatchOptions);
			// TODO: implement SSL options
			//var kafkaClient = new kafka.Client(this.kafkaZk, this.kafkaClientId, this.kafkaClientZkOptions, this.kafkaClientNoAckBatchOptions, this.kafkaClientSslOptions);

			var kafkaConsumer = new kafka.HighLevelConsumer(kafkaClient, kafkaPayloads, node.kafkaConsumerOptions);
			//var kafkaConsumer = new kafka.HighLevelConsumer(kafkaClient, [{topic: 'testtopic'}]);

	        node.status({fill: "green", shape: "dot", text: "connected to " + node.kafkaZk});

			kafkaConsumer.on('message', function(msg) {
				//node.send({payload: msg});
				node.send({payload: msg.value});
			});

			kafkaConsumer.on('error', function(err) {
				node.error(err);
			});

			kafkaConsumer.on('offsetOutOfRange', function(err) {
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