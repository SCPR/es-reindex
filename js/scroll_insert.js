var ScrollInsert, debug,
  __hasProp = {}.hasOwnProperty,
  __extends = function(child, parent) { for (var key in parent) { if (__hasProp.call(parent, key)) child[key] = parent[key]; } function ctor() { this.constructor = child; } ctor.prototype = parent.prototype; child.prototype = new ctor(); child.__super__ = parent.prototype; return child; };

debug = require("debug")("scpr");

module.exports = ScrollInsert = (function(_super) {
  __extends(ScrollInsert, _super);

  function ScrollInsert(es, idx) {
    this.es = es;
    this.idx = idx;
    this._batch = [];
    this._count = 0;
    ScrollInsert.__super__.constructor.call(this, {
      objectMode: true
    });
    this.once("finish", (function(_this) {
      return function() {
        return console.log("ScrollInsert finished with ", _this._count);
      };
    })(this));
  }

  ScrollInsert.prototype._write = function(obj, encoding, cb) {
    var wbatch;
    this._batch.push({
      index: {
        type: obj.type
      }
    });
    this._batch.push(obj.source);
    if (this._batch.length > 2000) {
      wbatch = this._batch.splice(0);
      debug("Would have inserted batch of " + (wbatch.length / 2));
      return es.bulk({
        index: this.idx,
        body: wbatch
      }, (function(_this) {
        return function(err, resp) {
          if (err) {
            console.error("Failed to bulk insert: ", err);
          }
          _this._count += wbatch.length / 2;
          return cb();
        };
      })(this));
    } else {
      return cb();
    }
  };

  return ScrollInsert;

})(require("stream").Writable);

//# sourceMappingURL=scroll_insert.js.map