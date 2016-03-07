net = require 'net'

tls = require 'tls'

TBufferedTransport = require './buffered_transport'

TBinaryProtocol = require './binary_protocol'

InputBufferUnderrunError = require './input_buffer_underrun_error'

exports.createMultiplexServer = (processor, options) ->
  serverImpl = (stream) ->
    self = this
    stream.on 'error', (err) -> self.emit 'error', err
    stream.on 'data', transport.receiver ((transportWithData) ->
      input = new protocol transportWithData
      output = new protocol new transport ``undefined``, (buf) ->
        try
          stream.write buf
        catch err
          self.emit 'error', err
          stream.end!
      try
        while true
          processor.process input, output
          transportWithData.commitPosition!
          break if not true
      catch err
        if err instanceof InputBufferUnderrunError
          transportWithData.rollbackPosition!
        else
          if err.message is 'Invalid type: undefined'
            transportWithData.rollbackPosition!
          else
            self.emit 'error', err
            stream.end!)
    stream.on 'end', -> stream.end!
    transport = if options and options.transport then options.transport else TBufferedTransport
    protocol = if options and options.protocol then options.protocol else TBinaryProtocol
    if options and options.tls then tls.createServer options.tls, serverImpl else net.createServer serverImpl

exports.createServer = (processor, handler, options) ->
  processor = processor.Processor if processor.Processor
  exports.createMultiplexServer (new processor handler), options
