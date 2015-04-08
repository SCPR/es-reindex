var ScrollInsert, ScrollSearch, argv, body, debug, elasticsearch, end_date, es, fs, indices, obj, query, runIndex, start_date, ts, tz, zone, _;

elasticsearch = require("elasticsearch");

fs = require("fs");

tz = require("timezone");

_ = require("underscore");

debug = require("debug")("scpr");

ScrollSearch = require("./scroll_search");

ScrollInsert = require("./scroll_insert");

argv = require('yargs').demand(['start', 'end', 'from', 'to']).describe({
  from: "Index to pull data from",
  to: "Index to insert data into",
  start: "Start Date",
  end: "End Date",
  zone: "Timezone",
  verbose: "Show Debugging Logs",
  filter: "ES JSON Filter",
  batch: "ES batch size"
}).boolean(['verbose'])["default"]({
  verbose: false,
  zone: "America/Los_Angeles",
  batch: 1000
}).argv;

if (argv.verbose) {
  (require("debug")).enable("scpr");
  debug = require("debug")("scpr");
}

zone = tz(require("timezone/" + argv.zone));

es = new elasticsearch.Client({
  host: "es-scpr-logstash.service.consul:9200"
});

start_date = zone(argv.start, argv.zone);

end_date = zone(argv.end, argv.zone);

query = argv.filter ? {
  filtered: {
    query: {
      match_all: {}
    },
    filter: JSON.parse(argv.filter)
  }
} : {
  match_all: {}
};

body = {
  query: query,
  size: argv.batch
};

debug("ES body is ", JSON.stringify(body));

indices = [];

ts = start_date;

while (true) {
  obj = {
    from: zone(ts, argv.from),
    to: zone(ts, argv.to)
  };
  debug("Prep: index " + obj.from + " -> " + obj.to);
  indices.push(obj);
  ts = tz(ts, "+1 day");
  if (ts >= end_date) {
    break;
  }
}

debug("Found " + indices.length + " index pairs.");

runIndex = function(cb) {
  var idx, search, writer;
  idx = indices.shift();
  if (!idx) {
    cb();
  }
  writer = new ScrollInsert(es, idx.to, argv.batch);
  search = new ScrollSearch(es, idx.from, body);
  search.pipe(writer);
  return search.once("end", function() {
    debug("Got search end.");
    return runIndex(cb);
  });
};

runIndex(function() {
  console.error("Done.");
  return process.exit(0);
});

//# sourceMappingURL=runner.js.map
