debug           = require("debug")("scpr")

ReadableSearch = require("elasticsearch-streams").ReadableSearch

module.exports = class ScrollSearch extends require('stream').PassThrough
    constructor: (@es,@idx,@body) ->
        super objectMode:true, highWaterMark:10000

        @_scrollId = null

        sFunc = (from,cb) =>
            if !@_scrollId
                debug "Executing initial search"
                @es.search index:@idx, body:@body, scroll:"60s", search_type:"scan", (err,resp) =>
                    throw err if err

                    debug "Total for #{ @idx } will be #{ resp.hits.total }."

                    @_scrollId = resp._scroll_id
                    sFunc null, cb

            else
                debug "Executing scroll"
                @es.scroll body:@_scrollId, scroll:"60s", (err,resp) =>
                    @_scrollId = resp._scroll_id
                    cb err, resp

        @_rs = new ReadableSearch sFunc
        @_rs.pipe(@)