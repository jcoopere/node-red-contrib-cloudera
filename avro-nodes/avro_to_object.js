module.exports = function(RED) {
	var avro = require('avro-js');

	/*
	 * Avro Converter Node
	 *
	 * Converts an Avro serialized object to a JavaScript object.
	 */
	function kafkaProducerNode(config) {
		RED.nodes.createNode(this, config);

		this.schema = config.schema;

		var type = avro.parse(this.schema);

		var node = this;

		try {
			this.on('input', function(msg) {
				if (Buffer.isBuffer(msg.payload)) {
					msg.payload = type.fromBuffer(msg.payload);
					node.send(msg);
				} else {
					node.error("Payload is not Avro: " + msg.payload);
				}
			});
		} catch (e) {
			node.error(e);
		}
	}
	RED.nodes.registerType("avro to object", kafkaProducerNode);
}