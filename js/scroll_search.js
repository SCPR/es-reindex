var ScrollSearch, debug,
  __hasProp = {}.hasOwnProperty,
  __extends = function(child, parent) { for (var key in parent) { if (__hasProp.call(parent, key)) child[key] = parent[key]; } function ctor() { this.constructor = child; } ctor.prototype = parent.prototype; child.prototype = new ctor(); child.__super__ = parent.prototype; return child; };

debug = require("debug")("scpr");

module.exports = ScrollSearch = (function(_super) {
  __extends(ScrollSearch, _super);

  function ScrollSearch(es, idx, body) {
    this.es = es;
    this.idx = idx;
    this.body = body;
    ScrollSearch.__super__.constructor.call(this, {
      objectMode: true
    });
    this._scrollId = null;
    this._total = null;
    this._remaining = null;
    this.__finished = false;
    this._fetching = false;
    this._keepFetching = true;
    this._fetch();
  }

  ScrollSearch.prototype._fetch = function() {
    if (this._fetching) {
      return false;
    }
    this._fetching = true;
    if (this._scrollId) {
      debug("Running scroll", this._scrollId);
      return this.es.scroll({
        scroll: "10s",
        body: this._scrollId
      }, (function(_this) {
        return function(err, results) {
          var r, _i, _len, _ref;
          if (err) {
            debug("Scroll failed: " + err);
            throw err;
          }
          debug("Scroll returned " + results.hits.hits.length + " results");
          if (results.hits.hits.length === 0) {
            return _this._finished();
          }
          _this._remaining -= results.hits.hits.length;
          _this._scrollId = results._scroll_id;
          _ref = results.hits.hits;
          for (_i = 0, _len = _ref.length; _i < _len; _i++) {
            r = _ref[_i];
            if (!_this.push({
              type: r._type,
              source: r._source
            })) {
              _this._keepFetching = false;
            }
          }
          if (_this._remaining <= 0) {
            return _this._finished();
          } else {
            _this._fetching = false;
            if (_this._keepFetching) {
              return _this._fetch();
            }
          }
        };
      })(this));
    } else {
      debug("Starting search on " + this.idx);
      return this.es.search({
        index: this.idx,
        body: this.body,
        search_type: "scan",
        scroll: "10s"
      }, (function(_this) {
        return function(err, results) {
          var r, _i, _len, _ref;
          if (err) {
            debug("Elasticsearch error: ", err);
            _this._finished();
            return false;
          }
          _this._total = results.hits.total;
          _this._remaining = results.hits.total - results.hits.hits.length;
          _this._scrollId = results._scroll_id;
          debug("First read. Total is " + _this._total + ".", _this._scrollId);
          _ref = results.hits.hits;
          for (_i = 0, _len = _ref.length; _i < _len; _i++) {
            r = _ref[_i];
            if (!_this.push({
              type: r._type,
              source: r._source
            })) {
              _this._keepFetching = false;
            }
          }
          if (_this._remaining <= 0) {
            return _this._finished();
          } else {
            _this._fetching = false;
            if (_this._keepFetching) {
              return _this._fetch();
            }
          }
        };
      })(this));
    }
  };

  ScrollSearch.prototype._read = function() {
    return this._fetch();
  };

  ScrollSearch.prototype._finished = function() {
    if (!this.__finished) {
      this.push(null);
      return this.__finished = true;
    }
  };

  return ScrollSearch;

})(require('stream').Readable);

//# sourceMappingURL=scroll_search.js.map
