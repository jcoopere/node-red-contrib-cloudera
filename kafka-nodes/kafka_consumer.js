module.exports = function(RED) {
	var kafka = require('kafka-node');

	/*
	 * Kafka Consumer Node
	 */
	function kafkaConsumerNode(config) {

	}
	RED.nodes.registerType("kafka consumer", kafkaConsumerNode);
}