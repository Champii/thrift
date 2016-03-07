util = require 'util'
EventEmitter = (require 'events').EventEmitter
net = require 'net'
tls = require 'tls'
thrift = require './thrift'
TBufferedTransport = require './buffered_transport'
TBinaryProtocol = require './binary_protocol'
InputBufferUnderrunError = require './input_buffer_underrun_error'
createClient = require './create_client'
binary = require './binary'

export class Connection
  (stream, options, dataHandler) ->
    # super!

    self = @
    @dataHandler = dataHandler
    @seqId2Service = {}
    @connection = stream
    @options = options or {}
    # @transport = @options.transport or TBufferedTransport
    # @protocol = @options.protocol or TBinaryProtocol
    @offline_queue = []
    @connected = false
    @initialize_retry_vars!
    @_debug = @options.debug or false
    @max_attempts = +@options.max_attempts if @options.max_attempts and not isNaN @options.max_attempts and @options.max_attempts > 0

    @retry_max_delay = null
    if @options.retry_max_delay isnt ``undefined`` and not isNaN @options.retry_max_delay and @options.retry_max_delay > 0
      @retry_max_delay = @options.retry_max_delay

    @connect_timeout = false
    if @options.connect_timeout and not isNaN @options.connect_timeout and @options.connect_timeout > 0
      @connect_timeout = +@options.connect_timeout

    @connection.addListener 'connect', ->
      self.connected = true
      @setTimeout self.options.timeout || 0
      @setNoDelay!
      @frameLeft = 0
      @framePos = 0
      @frame = null
      self.initialize_retry_vars!
      self.offline_queue.forEach ((data) -> self.connection.write data)
      # self.emit 'connect'

    @connection.addListener 'secureConnect', ->
      self.connected = true
      @setTimeout self.options.timeout || 0
      @setNoDelay!
      @frameLeft = 0
      @framePos = 0
      @frame = null
      self.initialize_retry_vars!
      self.offline_queue.forEach ((data) -> self.connection.write data)
      # self.emit 'connect'

    @connection.addListener 'error', (err) ->
      # self.emit 'error', err if (self.connection.listeners 'error').length is 1 || (self.listeners 'error').length > 0
      self.connection_gone!

    @connection.addListener 'close', -> self.connection_gone!

    # @connection.addListener 'timeout', -> self.emit 'timeout'

    # @connection.addListener 'data', -> self.emit \data, it
      #self.transport.receiver ((transport_with_data) ->
      # message = new self.protocol transport_with_data
      # try
      #   while true
      #     header = message.readMessageBegin!
      #     dummy_seqid = header.rseqid * -1
      #     client = self.client
      #     service_name = self.seqId2Service[header.rseqid]
      #     if service_name
      #       client = self.client[service_name]
      #       delete! self.seqId2Service[header.rseqid]
      #     client._reqs[dummy_seqid] = (err, success) ->
      #       transport_with_data.commitPosition!
      #       callback = client._reqs[header.rseqid]
      #       delete! client._reqs[header.rseqid]
      #       callback err, success if callback
      #     if client['recv_' + header.fname]
      #       client['recv_' + header.fname] message, header.mtype, dummy_seqid
      #     else
      #       delete! client._reqs[dummy_seqid]
            # self.emit 'error', new thrift.TApplicationException thrift.TApplicationExceptionType.WRONG_METHOD_NAME, 'Received a response to an unknown RPC function'
      # catch e
      #   if e instanceof InputBufferUnderrunError then transport_with_data.rollbackPosition! else throw e)

  end: -> @connection.end!

  initialize_retry_vars: ->
    @retry_timer = null
    @retry_totaltime = 0
    @retry_delay = 150
    @retry_backoff = 1.7
    @attempts = 0

  write: (data) ->
    if not @connected
      @offline_queue.push data
      return
    @connection.write data

  connection_gone: ->
    self = this
    return  if @retry_timer
    if not @max_attempts
      # self.emit 'close'
      return
    @connected = false
    @ready = false
    if @retry_max_delay isnt null and @retry_delay >= @retry_max_delay then @retry_delay = @retry_max_delay else @retry_delay = Math.floor @retry_delay * @retry_backoff
    if self._debug then console.log 'Retry connection in ' + @retry_delay + ' ms'
    if @max_attempts and @attempts >= @max_attempts
      @retry_timer = null
      console.error 'thrift: Couldn\'t get thrift connection after ' + @max_attempts + ' attempts.'
      # self.emit 'close'
      return
    @attempts += 1
    @emit 'reconnecting', {
      delay: self.retry_delay
      attempt: self.attempts
    }
    @retry_timer = setTimeout (->
      console.log 'Retrying connection...' if self._debug
      self.retry_totaltime += self.retry_delay
      if self.connect_timeout && self.retry_totaltime >= self.connect_timeout
        self.retry_timer = null
        console.error 'thrift: Couldn\'t get thrift connection after ' + self.retry_totaltime + 'ms.'
        # self.emit 'close'
        return
      self.connection.connect self.port, self.host
      self.retry_timer = null), @retry_delay

exports.createConnection = (host, port, options) ->
  stream = net.createConnection port, host
  connection = new Connection stream, options
  connection.host = host
  connection.port = port
  connection

exports.createSSLConnection = (host, port, options) ->
  stream = tls.connect port, host, options
  connection = new Connection stream, options
  connection.host = host
  connection.port = port
  connection

exports.createClient = createClient

child_process = require 'child_process'

StdIOConnection = exports.StdIOConnection = (command, options) ->
  command_parts = command.split ' '
  command = command_parts.0
  args = command_parts.splice 1, command_parts.length - 1
  child = @child = child_process.spawn command, args
  self = this
  EventEmitter.call this
  @_debug = options.debug or false
  @connection = child.stdin
  @options = options or {}
  @transport = @options.transport or TBufferedTransport
  @protocol = @options.protocol or TBinaryProtocol
  @offline_queue = []
  if @_debug is true
    @child.stderr.on 'data', (err) -> console.log err.toString!, 'CHILD ERROR'
    @child.on 'exit', (code, signal) -> console.log code + ':' + signal, 'CHILD EXITED'
  @frameLeft = 0
  @framePos = 0
  @frame = null
  @connected = true
  self.offline_queue.forEach ((data) -> self.connection.write data)
  # @connection.addListener 'error', (err) -> self.emit 'error', err
  # @connection.addListener 'close', -> self.emit 'close'
  child.stdout.addListener 'data', self.transport.receiver ((transport_with_data) ->
    message = new self.protocol transport_with_data
    try
      header = message.readMessageBegin!
      dummy_seqid = header.rseqid * -1
      client = self.client
      client._reqs[dummy_seqid] = (err, success) ->
        transport_with_data.commitPosition!
        callback = client._reqs[header.rseqid]
        delete! client._reqs[header.rseqid]
        callback err, success if callback
      client['recv_' + header.fname] message, header.mtype, dummy_seqid
    catch e
      if e instanceof InputBufferUnderrunError then transport_with_data.rollbackPosition! else throw e)

util.inherits StdIOConnection, EventEmitter

StdIOConnection::end = -> @connection.end!

StdIOConnection::write = (data) ->
  if not @connected
    @offline_queue.push data
    return
  @connection.write data

exports.createStdIOConnection = (command, options) -> new StdIOConnection command, options

exports.createStdIOClient = createClient
