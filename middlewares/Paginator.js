var moment = require('moment');

//This is a paginator for the connections stream
module.exports = function (req, res, next) {
  var interval = 20; // interval page
  var fragmentSize = 100; // size of Neighbouring Linked Connections Fragment that will be cut in multiple interval pages
  var pageSize = 20; // Same as interval basic LCFs
  //new page each X minutes - we can also get smarter when a profile is given which should be followed (TODO)
  if (!req.locals) {
    req.locals = {};
  }
  req.locals.page = {};
  req.locals.page.getInterval = function () {
    var dt = new Date(req.query.departureTime);
    if (req.query.departureStop) {
      // Calculate start and end departure time of page
      return {
        start : new Date(dt.getTime() + (req.query.page-1)*pageSize*60000), // Page interval represents minutes
        end : new Date(dt.getTime() + req.query.page*pageSize*60000)
      };
    } else {
      return {
        start : dt,
        end : new Date(dt.getTime() + interval * 60000)
      };
    }
  };
  req.locals.page.getDepartureStopInformation = function () {
    if (req.query.departureStop) {
      return {
        departureStop: req.query.departureStop,
        departureTime: new Date(req.query.departureTime),
        K : 5 // describes how many transfers are taken into account to reach a stop
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
    if (!req.query.departureStop) {
      return self._base + "/connections/?departureTime=" + encodeURIComponent(dt.add(interval, "minutes").format("YYYY-MM-DDTHH:mm"));
    } else {
      // The fragment exists of multiple pages so there's no surplus
      if (req.query.page*interval < fragmentSize || (!req.query.page || !isInt(req.query.page) || req.query.page == '1')) {
        return self._base + "/connections/?departureTime=" +  encodeURIComponent(dt.format("YYYY-MM-DDTHH:mm")) + "&departureStop=" + req.query.departureStop + "&page=" + (parseInt(req.query.page)+1) ;
      } else {
        return self._base + "/connections/?departureTime=" + encodeURIComponent(dt.add(fragmentSize, "minutes").format("YYYY-MM-DDTHH:mm"));
      }
    }
  }

  req.locals.page.getPreviousPage = function () {
    var dt = moment(req.query.departureTime);
    if (!req.query.departureStop) {
      return self._base + "/connections/?departureTime=" + encodeURIComponent(dt.format("YYYY-MM-DDTHH:mm"));
    } else {
      if (!req.query.page || !isInt(req.query.page) || req.query.page == '1') {
        return self._base + "/connections/?departureTime=" + encodeURIComponent(dt.subtract(interval, "minutes").format("YYYY-MM-DDTHH:mm"));
      } else {
        return self._base + "/connections/?departureTime=" +  encodeURIComponent(dt.format("YYYY-MM-DDTHH:mm")) + "&departureStop=" + req.query.departureStop + "&page=" + (parseInt(req.query.page)-1) ;
      }
    }
  }

  req.locals.page.getCurrentPage = function () {
    var dt = moment(req.query.departureTime);
    if (!req.query.departureStop) {
      return self._base + "/connections/?departureTime=" + encodeURIComponent(dt.format("YYYY-MM-DDTHH:mm"));
    } else {
      return self._base + "/connections/?departureTime=" + encodeURIComponent(dt.format("YYYY-MM-DDTHH:mm")) + "&departureStop=" + req.query.departureStop + "&page=" + req.query.page;
    }
  }

  function isInt(value) {
    return !isNaN(value) && (function(x) { return (x | 0) === x; })(parseFloat(value))
  }

  req.locals.page.setPage = function () {
    // If page is not set
    if (req.query.departureStop && (!req.query.page || !isInt(req.query.page))) {
      req.query.page = 1;
    } else if (req.query.departureStop && ((req.query.page-1) * pageSize >= fragmentSize)) {
      // If page exceeds the fragment size
      // Use basic LCFs
      req.query.departureStop = null;
      req.query.departureTime = moment(req.query.departureTime).add(fragmentSize, "minutes");
    }
  }

  next();
}
