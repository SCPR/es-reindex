debug           = require("debug")("scpr")

module.exports = class ScrollSearch extends require('stream').Readable
    constructor: (@es,@idx,@body) ->
        super objectMode:true, highWaterMark:10000

        @_scrollId  = null
        @_total     = null
        @_remaining = null

        @__finished = false

        @_fetching      = false
        @_keepFetching  = true

        @_fetch()

    _fetch: ->
        if @_fetching
            return false

        @_fetching = true

        if @_scrollId
            debug "Running scroll", @_scrollId
            @es.scroll scroll:"60s", body:@_scrollId, (err,results) =>
                if err
                    debug "Scroll failed: #{err}"
                    throw err

                debug "Scroll returned #{ results.hits.hits.length } results"
                if results.hits.hits.length == 0
                    return @_finished()

                @_remaining -= results.hits.hits.length
                @_scrollId  = results._scroll_id

                for r in results.hits.hits
                    @_keepFetching = false if !@push type:r._type, source:r._source

                if @_remaining <= 0
                    @_finished()
                else
                    @_fetching = false
                    @_fetch() if @_keepFetching

                    if !@_keepFetching
                        debug "Suspending fetches after a push returned false"

        else
            debug "Starting search on #{@idx}"
            @es.search index:@idx, body:@body, search_type:"scan", scroll:"60s", (err,results) =>
                if err
                    # FIXME: The most likely case here is connection failure or IndexMissing
                    debug "Elasticsearch error: ", err
                    @_finished()
                    return false

                @_total     = results.hits.total
                @_remaining = results.hits.total - results.hits.hits.length
                @_scrollId  = results._scroll_id

                debug "First read. Total is #{ @_total }.", @_scrollId

                for r in results.hits.hits
                    @_keepFetching = false if !@push type:r._type, source:r._source, id:r._id

                if @_remaining <= 0
                    @_finished()
                else
                    @_fetching = false
                    @_fetch() if @_keepFetching


    _read: ->
        @_fetch()

    _finished: ->
        if !@__finished
            @push null
            @__finished = true