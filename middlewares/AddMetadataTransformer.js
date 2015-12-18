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
    connection["countDirectStopsArrivalStop"] = this._neighbours[connection["arrivalStop"]].count_direct_stops.toString();
    connection["countDirectStopsDepartureStop"] = this._neighbours[connection["departureStop"]].count_direct_stops.toString();
  }

  // Coordinates
  if (this._coordinates) {
    connection["locationArrivalStop"] = {};
    connection["locationArrivalStop"].longitude = this._coordinates[connection["arrivalStop"]].longitude.toString();
    connection["locationArrivalStop"].latitude = this._coordinates[connection["arrivalStop"]].latitude.toString();
    connection["locationDepartureStop"] = {};
    connection["locationDepartureStop"].longitude = this._coordinates[connection["departureStop"]].longitude.toString();
    connection["locationDepartureStop"].latitude = this._coordinates[connection["departureStop"]].latitude.toString();
  }
  done(null, connection);
}

module.exports = AddMetadataTransformer;
