var Transform = require('stream').Transform,
    util = require('util');

util.inherits(AddMetadataTransformer, Transform);

function AddMetadataTransformer (neighbours, coordinates) {
  this._neighbours = neighbours;
  this._coordinates = coordinates;
  Transform.call(this, {objectMode : true});
}

AddMetadataTransformer.prototype._transform = function (connection, encoding, done) {
  // Metadata amount of direct stops
  if (this._neighbours) {
    connection["arrivalStopCountDirectStops"] = this._neighbours[connection["arrivalStop"]].count_direct_stops.toString();
    connection["departureStopCountDirectStops"] = this._neighbours[connection["departureStop"]].count_direct_stops.toString();
  }

  // Coordinates
  if (this._coordinates) {
    connection["departureStopLongitude"] = this._coordinates[connection["departureStop"]].longitude.toString();
    connection["departureStopLatitude"] = this._coordinates[connection["departureStop"]].latitude.toString();
    connection["arrivalStopLongitude"] = this._coordinates[connection["arrivalStop"]].longitude.toString();
    connection["arrivalStopLatitude"] = this._coordinates[connection["arrivalStop"]].latitude.toString();
  }
  done(null, connection);
}

module.exports = AddMetadataTransformer;
