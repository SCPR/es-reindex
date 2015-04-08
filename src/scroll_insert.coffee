debug           = require("debug")("scpr")

module.exports = class ScrollInsert extends require("stream").Writable
    constructor: (@es,@idx) ->
        @_batch = []
        @_count = 0
        super objectMode:true

        @once "finish", =>
            console.log "ScrollInsert finished with ", @_count

    _write: (obj,encoding,cb) ->
        @_batch.push index:{type:obj.type}
        @_batch.push obj.source

        if @_batch.length > 2000
            wbatch = @_batch.splice(0)
            debug "Would have inserted batch of #{ wbatch.length / 2 }"
            es.bulk index:@idx, body:wbatch, (err,resp) =>
                if err
                    console.error "Failed to bulk insert: ", err

                @_count += (wbatch.length / 2)

                cb()
        else
            cb()