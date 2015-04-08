debug           = require("debug")("scpr")

module.exports = class ScrollInsert extends require("stream").Writable
    constructor: (@es,@idx) ->
        @_batch = []
        @_count = 0
        super objectMode:true

        @once "finish", =>
            console.log "ScrollInsert finished with ", @_count

    _write: (obj,encoding,cb) ->
        @_batch.push index:{ _type:obj.type, _id:obj.id }
        @_batch.push obj.source

        if @_batch.length > 2000
            wbatch = @_batch.splice(0)
            @es.bulk index:@idx, body:wbatch, (err,resp) =>
                if err
                    console.error "Failed to bulk insert: ", err

                @_count += (wbatch.length / 2)
                debug "Inserted batch of #{ wbatch.length / 2 }. Total is now #{ @_count }"

                cb()
        else
            cb()