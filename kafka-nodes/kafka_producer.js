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
	 * Kafka Producer Node
	 */
	function kafkaProducerNode(config) {
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

		var kafkaClient = new kafka.Client(this.kafkaZk, null, this.kafkaClientZkOptions, this.kafkaClientNoAckBatchOptions);
		// TODO: implement SSL options
		//var kafkaClient = new kafka.Client(this.kafkaZk, this.kafkaClientId, this.kafkaClientZkOptions, this.kafkaClientNoAckBatchOptions, this.kafkaClientSslOptions);

		this.kafkaProducerOptions = {};
		if (config.requireAcks) this.kafkaProducerOptions.requireAcks = config.requireAcks;
		if (config.ackTimeoutMs) this.kafkaProducerOptions.ackTimeoutMs = config.ackTimeoutMs;
		if (config.partitionerType) this.kafkaProducerOptions.partitionerType = config.partitionerType;

		var kafkaProducer = new kafka.HighLevelProducer(kafkaClient, this.kafkaProducerOptions);

        this.status({fill: "green", shape: "dot", text: "connected to " + node.kafkaZk});

		try {
			this.on('input', function(msg) {
				var payload = msg.payload;

				// If msg.payload is of type 'object' and is NOT a Buffer, it should be stringified.
				// Otherwise, the value of the Kafka message will be "[object Object]".
				if (typeof(payload) == 'object') {
					if (!Buffer.isBuffer(payload)) {
						payload = JSON.stringify(payload);
					}
				}

				// TODO: ProduceRequest can also have properties for "key" (for keyed partitioning) and "attributes"
				var kafkaPayloads = [];
				node.kafkaTopics.forEach(function(tpc) {
					kafkaPayloads.push({topic: tpc, messages: payload})
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