require! {
  net
  events: {EventEmitter}
  \./buffered_transport : TBufferedTransport
  \./binary_protocol : BinaryProtocol
  \./connection : {Connection}
  \./input_buffer_underrun_error : InputBufferUnderrunError

  \./gen-nodejs/Test
  \prelude-ls
}

global import prelude-ls

class Socket extends EventEmitter
  -> @sockets = []; @events = []
  Each: -> each it, @sockets
  Add: -> @sockets.push it
  Remove: (socket) -> @sockets = reject (is socket), @sockets
  Handler: ->
    (socket) ~>
      @Add socket
      @emit \connect socket

class Client extends Socket
  (@host = \localhost, @port = 1234) ->
    super!
    socket = net.createConnection @port, @host, ~>
      @Add socket
      @emit \connect socket
      @client = new Connection socket

class Server extends Socket
  (@port = 1234) ->
    super!
    @server = net.createServer @Handler!
    @server.listen @port

class End
  (@Processor, @socket, @handler, @Transport = TBufferedTransport, @Protocol = BinaryProtocol) ->

class Receiver extends End
  ->
    super ...

    @Processor = @Processor.Processor
    # @socket.on 'error', (err) -> self.emit 'error', err
    @socket.on \connect (socket) ~>
      processor = new @Processor @handler
      socket.on \data @Transport.receiver (transportWithData) ~>
        input = new @Protocol transportWithData
        output = new @Protocol new @Transport undefined, (buf) ~>
          try
            socket.write buf
          catch err
            @emit 'error', err
            socket.end!
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
              console.log err
              # @emit 'error', err
              socket.end!

class Emitter extends End
  ->
    super ...
    # @Extend!
    @Processor = @Processor.Client
    @socket.on \connect (socket) ~>
      transport = new @Transport undefined, (buf, seqid) ~> socket.write buf, seqid
      processor = new @Processor transport, @Protocol
      @handler processor

      socket.on \data @Transport.receiver (transport_with_data) ~>
        message = new @Protocol transport_with_data
        try
          while true
            header = message.readMessageBegin!
            dummy_seqid = header.rseqid * -1
            # client = @client
            # service_name = @seqId2Service[header.rseqid]
            # if service_name
            #   client = @client[service_name]
            #   delete! @seqId2Service[header.rseqid]
            processor._reqs[dummy_seqid] = (err, success) ~>
              transport_with_data.commitPosition!
              callback = processor._reqs[header.rseqid]
              delete! processor._reqs[header.rseqid]
              callback err, success if callback
            if processor['recv_' + header.fname]
              processor['recv_' + header.fname] message, header.mtype, dummy_seqid
            else
              delete! processor._reqs[dummy_seqid]
              # @emit 'error', new thrift.TApplicationException thrift.TApplicationExceptionType.WRONG_METHOD_NAME, 'Received a response to an unknown RPC function'
        catch e
          if e instanceof InputBufferUnderrunError then transport_with_data.rollbackPosition! else throw e

# server
serv1 = new Server 9001
serv2 = new Server 9002

servEmtr = new Emitter Test, serv2, (client) ->
  client.test 42, (err, res) -> console.log "RETURNSERVER" err, res

servHandler =
  test: (arg, done) ->
    console.log 'Test SERVER!' arg
    done null \TadaServer!

servRecv = new Receiver Test, serv1, servHandler

# client
cli1 = new Client \localhost 9001
cli2 = new Client \localhost 9002

cliEmtr = new Emitter Test, cli1, (client) ->
  client.test 12, (err, res) -> console.log "RETURNCLIENT" err, res

cliHandler =
  test: (arg, done) ->
    console.log 'Test CLIENT!' arg
    done null \TadaClient!

cliRecv = new Receiver Test, cli2, cliHandler
