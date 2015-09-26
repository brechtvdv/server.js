var MongoClient = require('mongodb').MongoClient,
    fs = require('fs');

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

////////////////////////////////////////
// Methods for retrieving connections //
////////////////////////////////////////
MongoDBConnector.connectionsContext = function (callback) {
  // Get context
  this._db.collection(this.collections['connections']).find({'@context': { '$exists': true}}, {'_id': 0}).toArray(function(err, context) {
    if (err) {
      console.error(err);
      callback(null);
    } else if (context && context[0]) {
      callback(context[0]);
    } else {
      console.error("No context found in collection");
      callback(null);
    }
  });
};

/**
 * @param page is an object describing the page of the resource
 */
MongoDBConnector._getMongoConnectionsStream = function (page, cb) {
  var self = this;

  // Get connections stream with context added
  var connectionsStream = self._db.collection(self.collections['connections'])
      .find({'departureTime': {'$gt': page.getInterval().start, '$lt': page.getInterval().end}})
      .sort({'departureTime': 1})
      .stream({
        transform: function(connection) {
          connection['@id'] = connection['_id'];
          delete connection['_id'];
          return connection;
        }
    });
  cb(null, connectionsStream);
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

////////////////////////////////////////
// Methods for retrieving stops       //
////////////////////////////////////////
MongoDBConnector.stopsContext = function (callback) {
  callback(null);
};

/**
 * @param page is an object describing the page of the resource
 */
MongoDBConnector._getMongoStopsStream = function (page, cb) {
  var self = this;

  // Use page to retrieve interval [offset, offset+limit]
  var options = {
    'limit': parseInt(page.getLimit()),
    'skip': parseInt(page.getOffset()),
    'sort': 'stop_name'
  };
  
  var stopsStream = self._db.collection(self.collections['stops'])
      .find({}, options)
      .stream({
        transform: function(stop) {
          stop['@id'] = stop['stop_id'];
          delete stop['_id'];
          return stop;
        }
      });
  cb(null, stopsStream);
};

MongoDBConnector.getStopsPage = function (page, cb) {
  var stream = this._getMongoStopsStream(page, function (error, stopsStream) {
    if (error) {
      cb (error);
    } else {
      cb(null, stopsStream);
    }
  });
};

MongoDBConnector.getStop = function (stopid, cb) {

}

MongoDBConnector.addStop = function (stop, cb) {

};

module.exports = MongoDBConnector;
