elasticsearch   = require "elasticsearch"
fs              = require "fs"
tz              = require "timezone"
_ = require "underscore"

debug           = require("debug")("scpr")

ScrollSearch = require "./scroll_search"
ScrollInsert = require "./scroll_insert"

argv = require('yargs')
    .demand(['start','end','from','to'])
    .describe
        from:       "Index to pull data from"
        to:         "Index to insert data into"
        start:      "Start Date"
        end:        "End Date"
        zone:       "Timezone"
        verbose:    "Show Debugging Logs"
        filter:     "ES JSON Filter"
        batch:      "ES batch size"
    .boolean(['verbose'])
    .default
        verbose:    false
        zone:       "America/Los_Angeles"
        batch:      1000
    .argv

if argv.verbose
    (require "debug").enable("scpr")
    debug = require("debug")("scpr")

zone = tz(require("timezone/#{argv.zone}"))

es = new elasticsearch.Client host:"es-scpr-logstash.service.consul:9200"

start_date  = zone(argv.start,argv.zone)
end_date    = zone(argv.end,argv.zone)

# -- Build our Query -- #

query = if argv.filter
    filtered:
        query:
            match_all: {}
        filter: JSON.parse(argv.filter)
else
    match_all: {}

body =
    query:  query
    size:   argv.batch

debug "ES body is ", JSON.stringify(body)

# -- Build an array of index pairs -- #

indices = []

ts = start_date

loop
    obj = from:zone(ts,argv.from), to:zone(ts,argv.to)
    debug "Prep: index #{obj.from} -> #{obj.to}"
    indices.push obj
    ts = tz(ts,"+1 day")
    break if ts >= end_date

debug "Found #{ indices.length } index pairs."

# -- Run -- #

runIndex = (cb) ->
    idx = indices.shift()

    if !idx
        # we're all done
        cb()


    writer = new ScrollInsert es, idx.to, argv.batch
    search = new ScrollSearch es, idx.from, body

    search.pipe(writer)

    search.once "end", ->
        debug "Got search end."
        runIndex cb

runIndex ->
    console.error "Done."
    process.exit(0)





