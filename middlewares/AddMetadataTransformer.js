var Transform = require('stream').Transform,
    util = require('util');

util.inherits(AddMetadataTransformer, Transform);

function AddMetadataTransformer (neighbours) {
  this._neighbours = neighbours;
  Transform.call(this, {objectMode : true});
}

AddMetadataTransformer.prototype._transform = function (connection, encoding, done) {
  connection["arrivalStop_count_direct_stops"] = this._neighbours[connection["arrivalStop"]].count_direct_stops.toString();
  connection["departureStop_count_direct_stops"] = this._neighbours[connection["departureStop"]].count_direct_stops.toString();
  done(null, connection);
}

module.exports = AddMetadataTransformer;
