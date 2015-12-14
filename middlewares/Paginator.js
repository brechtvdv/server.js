var moment = require('moment');

//This is a paginator for the connections stream
module.exports = function (req, res, next) {
  var interval = 10; // interval X
  //new page each X minutes - we can also get smarter when a profile is given which should be followed (TODO)
  if (!req.locals) {
    req.locals = {};
  }
  req.locals.page = {};
  req.locals.page.getInterval = function () {
    var dt = new Date(req.query.departureTime);
    return {
      start : dt,
      end : new Date(dt.getTime() + interval * 60000)
    };
  };
  req.locals.page.getDepartureStopInformation = function () {
    if (req.query.departureStop) {
      return {
        range : 3, // describes how many stops futher from the departure stop connections may depart from
        interval : interval, // Interval with the optimization should be much bigger
        departureStop : req.query.departureStop
      };
    } else {
      return null;
    }
  };
  this._base = req.locals.config.baseUri;
  if (req.port) {
    this._base += ':' + req.port;
  }
  var self = this;
  req.locals.page.getCorrectPageId = function (dt) {
    if (!dt) {
      dt = moment(req.query.departureTime);
    } else {
      dt = moment(dt);
    }

    // Round minutes down with modulus of X
    var minutes = dt.minutes();
    minutes %= interval;
    dt.subtract(minutes, 'minutes');
    return dt.format("YYYY-MM-DDTHH:mm");
  };

  req.locals.page.getNextPage = function () {
    var dt = moment(req.query.departureTime);
    return self._base + "/connections/?departureTime=" + encodeURIComponent(dt.add(interval, "minutes").format("YYYY-MM-DDTHH:mm"));
  }

  req.locals.page.getPreviousPage = function () {
    var dt = moment(req.query.departureTime);
    return self._base + "/connections/?departureTime=" +  encodeURIComponent(dt.subtract(interval, "minutes").format("YYYY-MM-DDTHH:mm"));
  }

  req.locals.page.getCurrentPage = function () {
    var dt = moment(req.query.departureTime);
    return self._base + "/connections/?departureTime=" + encodeURIComponent(dt.format("YYYY-MM-DDTHH:mm"));
  }

  next();
}
