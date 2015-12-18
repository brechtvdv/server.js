var MongoClient = require('mongodb').MongoClient,
    MongoDBFixStream = require('./MongoDBFixStream'),
    AddMetadataTransformer = require('./AddMetadataTransformer'),
    Stream = require('stream');
var MongoDBConnector = function () {
  return function (req, res, next) {
    req.db = MongoDBConnector;
    next();
  }
}

/**
 * Connect to the mongodb if not yet connected.
 * @param dbstring defines the mongoclient string to connect to mongodb
 * @param collections is an object of
 * @param cb is a callback that needs to be called without parameters when the connection was succesful, or with 1 parameter when an error was encountered
 */
MongoDBConnector.connect = function (dbstring, collections, cb) {
  this.collections = collections;
  //check if we already have a db connection
  if (typeof this._db !== 'undefined') {
    cb();
  } else {
    var self = this;
    MongoClient.connect(dbstring, function(err, db) {
      if (err) {
        cb('Error connecting to the db: ' + err);
      }
      self._db = db;
      // Create index for connections
      db.collection(collections['connections']).ensureIndex({'departureTime': 1, 'departureStop': 1}, function (err) {
        if (err) {
          console.error(err);
        }
        // Create index for neighbours search
        self._db.collection(collections['neighbours']).ensureIndex({'stop_id': 1}, function(err) {
          if (err) {
            console.error(err);
          }
          cb();
        });
      });
    });
  }
};

MongoDBConnector.context = function (callback) {
  callback({"@context" : { "lc" : "http://semweb.mmlab.be/ns/linkedconnections#", "gtfs" : "http://vocab.gtfs.org/terms#", "Connection" : "http://semweb.mmlab.be/ns/linkedconnections#Connection", "dct" : "http://purl.org/dc/terms/", "date" : "dct:date", "arrivalTime" : "lc:arrivalTime", "departureTime" : "lc:departureTime", "arrivalStop" : { "@type" : "@id", "@id" : "http://semweb.mmlab.be/ns/linkedconnections#arrivalStop" }, "departureStop" : { "@type" : "@id", "@id" : "http://semweb.mmlab.be/ns/linkedconnections#departureStop" }, "trip" : { "@type" : "@id", "@id" : "gtfs:trip" }, "route" : { "@type" : "@id", "@id" : "gtfs:route" }, "headsign" : "gtfs:headsign","locationDepartureStop": { "@context": "http://schema.org/", "@id": "http://semweb.mmlab.be/ns/linkedconnections#locationDepartureStop", "@type": "GeoCoordinates", "latitude": "https://schema.org/GeoCoordinates#latitude", "longitude": "https://schema.org/GeoCoordinates#longitude" }, "locationArrivalStop": { "@context": "http://schema.org/", "@id": "http://semweb.mmlab.be/ns/linkedconnections#locationArrivalStop", "@type": "GeoCoordinates", "latitude": "https://schema.org/GeoCoordinates#latitude", "longitude": "https://schema.org/GeoCoordinates#longitude" }, "countDirectStopsArrivalStop": "http://semweb.mmlab.be/ns/linkedconnections#countDirectStopsArrivalStop", "countDirectStopsDepartureStop": "http://semweb.mmlab.be/ns/linkedconnections#countDirectStopsDepartureStop"} });
};

/**
 * @param page is an object describing the page of the resource
 */
MongoDBConnector._getMongoConnectionsStream = function (page, cb) {
  var self = this;
  self._cb = cb;
  // When only queried on departure time
  if (page.getDepartureStopInformation() == null) {
    var connectionsStream = this._db.collection(this.collections['connections'])
        .find({'departureTime': {'$gte': page.getInterval().start, '$lt': page.getInterval().end}})
        .sort({'departureTime': 1})
        .stream().pipe(new MongoDBFixStream());
    cb(null, connectionsStream);
  } else {
    var info = page.getDepartureStopInformation();
    var departureStop = info.departureStop;
    var departureTime = page.getInterval().start;
    var interval = info.interval;
    var K = info.range;
    this._getNeighbouringConnections(departureTime, departureStop, interval, K, cb);
  }
};

MongoDBConnector.getConnectionsPage = function (page, cb) {
  var stream = this._getMongoConnectionsStream(page, function (error, connectionsStream) {
    if (error) {
      cb (error);
    } else {
      cb(null, connectionsStream);
    }
  });
};

/**
 * Returns connections in a certain connection range around a stop
 * @param departureTime is an object describing the time of departure at the departure stop
 * @param departureStop is the stop of departuring
 * @param K describes how many transfers are necessary between a departure stop and another stop
 */
MongoDBConnector._getNeighbouringConnections = function (departureTime, departureStop, interval, K, cb) {
  var self = this;
  var coordinates = {}; // holds for every neighbour its coordinates
  var endDepartureTime = new Date(departureTime.getTime() + interval * 60000);
  var queryOr = [{'departureTime': {'$gte': departureTime, '$lt': endDepartureTime}, 'departureStop': departureStop}]; // holds the WHERE-clausule for the query
  // Get connections within a certain range around the stop
  this._db.collection(this.collections['neighbours']).findOne({'stop_id': departureStop}, function(err, stop) {
      if (stop) {
        // Add coordinates of departure stop
        coordinates[stop.stop_id] = {};
        coordinates[stop.stop_id].longitude = stop.longitude;
        coordinates[stop.stop_id].latitude = stop.latitude;

        for (var j = 0; j < Object.keys(stop.neighbours).length; j++) {
          var neighbourStopId = Object.keys(stop.neighbours)[j].toString();
          var neighbour = stop.neighbours[neighbourStopId];

          // Save coordinates
          if (neighbour.radius <= K+1 != coordinates[neighbourStopId]) {
            coordinates[neighbourStopId] = {};
            coordinates[neighbourStopId].longitude = neighbour.longitude;
            coordinates[neighbourStopId].latitude = neighbour.latitude;
          }
          if (K > 0 && neighbour.radius <= K) {
            // Build query selector
            var startDepartureTime = new Date(departureTime.getTime() + neighbour.timedistance * 1000); // time offset is in seconds
            var endDepartureTime = new Date(startDepartureTime.getTime() + interval * 60000);
            queryOr.push({'departureTime': {'$gte': startDepartureTime, '$lt': endDepartureTime}, 'departureStop': neighbourStopId});
          }
        }
        // Query connections
        var connectionsStream = self._db.collection(self.collections['connections']).find({ $or : queryOr, 'arrivalStop' : { '$ne' : departureStop }}).sort({'departureTime': 1}).stream().pipe(new MongoDBFixStream()).pipe(new AddMetadataTransformer(stop.neighbours, coordinates));
        cb(null, connectionsStream);
      } else {
        // stop is not known by this server
        var emptyStream = new Stream.Readable({
          read: function(chunk, encoding, next) {
            this.push(null);
          }
        });
        cb(null, emptyStream);
      }
  });
};

MongoDBConnector.getStops = function (cb) {
  this._db.collection(this.collections['stops']).find().toArray(cb);
};

module.exports = MongoDBConnector;
