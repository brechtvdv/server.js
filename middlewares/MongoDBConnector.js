var MongoClient = require('mongodb').MongoClient,
    MongoDBFixStream = require('./MongoDBFixStream'),
    AddMetadataTransformer = require('./AddMetadataTransformer');
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
      cb();
    });
  }
};

MongoDBConnector.context = function (callback) {
  callback({"@context" : { "lc" : "http://semweb.mmlab.be/ns/linkedconnections#", "gtfs" : "http://vocab.gtfs.org/terms#", "Connection" : "http://semweb.mmlab.be/ns/linkedconnections#Connection", "dct" : "http://purl.org/dc/terms/", "date" : "dct:date", "arrivalTime" : "lc:arrivalTime", "departureTime" : "lc:departureTime", "arrivalStop" : { "@type" : "@id", "@id" : "http://semweb.mmlab.be/ns/linkedconnections#arrivalStop" }, "departureStop" : { "@type" : "@id", "@id" : "http://semweb.mmlab.be/ns/linkedconnections#departureStop" }, "trip" : { "@type" : "@id", "@id" : "gtfs:trip" }, "route" : { "@type" : "@id", "@id" : "gtfs:route" }, "headsign" : "gtfs:headsign" }});
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
 * @param K holds the connection radius
 */
MongoDBConnector._getNeighbouringConnections = function (departureTime, departureStop, interval, K, cb) {
  var self = this;
  var endDepartureTime = new Date(departureTime.getTime() + interval * 60000);
  var queryOr = [{'departureTime': {'$gte': departureTime, '$lt': endDepartureTime}, 'departureStop': departureStop}]; // holds the WHERE-clausule for the query
  if (K > 1) {
    // Get connections within a certain range around the stop
    this._db.collection(this.collections['neighbours']).findOne({'stop_id': departureStop}, function(err, stop) {
      // Build query for every neighbour
      for (var j = 0; j < Object.keys(stop.neighbours).length; j++) {
        var neighbourStopId = Object.keys(stop.neighbours)[j];
        var neighbour = stop.neighbours[neighbourStopId];
        if (neighbour.radius <= K) {
          // Build query selector
          var startDepartureTime = new Date(departureTime.getTime() + neighbour.timedistance * 1000); // time offset is in seconds
          var endDepartureTime = new Date(startDepartureTime.getTime() + interval * 60000);
          queryOr.push({'departureTime': {'$gte': startDepartureTime, '$lt': endDepartureTime}, 'departureStop': neighbourStopId});
        }
      }
      // Query connections
      var connectionsStream = self._db.collection(self.collections['connections']).find({ $or : queryOr, 'arrivalStop' : { '$ne' : departureStop }}).sort({'departureTime': 1}).stream().pipe(new MongoDBFixStream()).pipe(new AddMetadataTransformer(stop.neighbours));
      cb(null, connectionsStream);
    });
  } else {
    // Query connections
    var connectionsStream = self._db.collection(self.collections['connections']).find({ $or : queryOr}).sort({'departureTime': 1}).stream().pipe(new MongoDBFixStream()).pipe(new AddMetadataTransformer(stop.neighbours));
    cb(null, connectionsStream);
  }
};

MongoDBConnector.getStops = function (cb) {
  this._db.collection(this.collections['stops']).find().toArray(cb);
};

module.exports = MongoDBConnector;
