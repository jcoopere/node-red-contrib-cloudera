/*
Licensed to Cloudera, Inc. under one
or more contributor license agreements. See the NOTICE file
distributed with this work for additional information
regarding copyright ownership. Cloudera, Inc. licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License. You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/
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

		this.kafkaClientZkOptions = {};
		if (config.zkSessionTimeout) this.kafkaClientZkOptions.sessionTimeout = config.zkSessionTimeout;
		if (config.zkSpinDelay) this.kafkaClientZkOptions.spinDelay = config.zkSpinDelay;
		if (config.zkRetries) this.kafkaClientZkOptions.retries = config.zkRetries;

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
				/* N.B. 
				 A Kafka message is a JavaScript Object including the following properties:
				  - topic (name of the topic the message was consumed from)
				  - value (message body)
				  - offset (message offset)
				  - partition (partition message was consumed from)
				  - key (key of the message)
				*/
				node.send({payload: msg});
			});

			kafkaConsumer.on('error', function(err) {
				node.error(err);
				/*if (err == "FailedToRebalanceConsumerError: Exception: NODE_EXISTS[-110]") {
					node.warn("Failed to rebalance consumer detected. Pausing for 60 seconds and reconnecting...");
					setTimeout(function() { node.warn("Recreating consumer..."); kafkaConsumer = new kafka.HighLevelConsumer(kafkaClient, kafkaPayloads, node.kafkaConsumerOptions); }, 60000);
				}*/
			});

			kafkaConsumer.on('offsetOutOfRange', function(err) {
				node.error(err);
			});

			this.on('close', function(done) {
				kafkaConsumer.close(true, cb);
				kafkaClient.close(cb);
			});
		} catch (err) {
			node.error(err);
		}
	}
	RED.nodes.registerType("kafka consumer", kafkaConsumerNode);
}