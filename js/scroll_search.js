var ReadableSearch, ScrollSearch, debug,
  __hasProp = {}.hasOwnProperty,
  __extends = function(child, parent) { for (var key in parent) { if (__hasProp.call(parent, key)) child[key] = parent[key]; } function ctor() { this.constructor = child; } ctor.prototype = parent.prototype; child.prototype = new ctor(); child.__super__ = parent.prototype; return child; };

debug = require("debug")("scpr");

ReadableSearch = require("elasticsearch-streams").ReadableSearch;

module.exports = ScrollSearch = (function(_super) {
  __extends(ScrollSearch, _super);

  function ScrollSearch(es, idx, body) {
    var sFunc;
    this.es = es;
    this.idx = idx;
    this.body = body;
    ScrollSearch.__super__.constructor.call(this, {
      objectMode: true,
      highWaterMark: 10000
    });
    this._scrollId = null;
    sFunc = (function(_this) {
      return function(from, cb) {
        if (!_this._scrollId) {
          debug("Executing initial search");
          return _this.es.search({
            index: _this.idx,
            body: _this.body,
            scroll: "60s",
            search_type: "scan"
          }, function(err, resp) {
            if (err) {
              throw err;
            }
            debug("Total for " + _this.idx + " will be " + resp.hits.total + ".");
            _this._scrollId = resp._scroll_id;
            return sFunc(null, cb);
          });
        } else {
          debug("Executing scroll");
          return _this.es.scroll({
            body: _this._scrollId,
            scroll: "60s"
          }, function(err, resp) {
            _this._scrollId = resp._scroll_id;
            return cb(err, resp);
          });
        }
      };
    })(this);
    this._rs = new ReadableSearch(sFunc);
    this._rs.pipe(this);
  }

  return ScrollSearch;

})(require('stream').PassThrough);

//# sourceMappingURL=scroll_search.js.map
