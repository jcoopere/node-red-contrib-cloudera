module.exports = function(RED) {
	var avro = require('avro-js');

	/*
	 * Avro Converter Node
	 *
	 * Converts JSON to Avro and Avro to JSON.
	 */
	function avroNode(config) {
		RED.nodes.createNode(this, config);

		this.schemaLiteral = config.schemaLiteral;
		this.schemaFile = config.schemaFile;
		this.schemaSelector = config.schemaSelector;

		var node = this;

		var schema = config.schemaLiteral;

		if (!schema) {
			node.error("No Avro schema provided.");
		}

		var type = avro.parse(schema);

		try {
			this.on('input', function(msg) {
				if (typeof(msg.payload) == 'object') {
					if (Buffer.isBuffer(msg.payload)) {
						msg.payload = type.fromBuffer(msg.payload);
						node.send(msg);
					} else if (type.isValid(msg.payload)) {
						msg.payload = type.toBuffer(msg.payload);
						node.send(msg);
					} else {
						node.error("msg.payload object is not valid for the provided Avro schema: " + JSON.stringify(msg.payload));
					}
				} else {
					node.error("msg.payload is not an object, type is: " + typeof(msg.payload));
				}
			});
		} catch (e) {
			node.error(e);
		}
	}
	RED.nodes.registerType("avro", avroNode);
}