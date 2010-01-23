# 
# File: handler.rb
#
require 'eventmachine'
require 'roma/command/receiver'
require 'roma/event/con_pool'
require 'roma/messaging/con_pool'
require 'roma/logging/rlogger'
require 'socket'

module Roma
  module Event

    class Handler < EventMachine::Connection

      attr_accessor :stop_event_loop
      attr :connected
      attr :fiber
      attr :rbuf

      attr_accessor :timeout
      attr_reader :lastcmd

      def initialize(storages, rttable)
        @rbuf=''
        @timeout = 10
        @log = Roma::Logging::RLogger.instance

        @receiver = Roma::Command::Receiver.new(self, storages, rttable)
      end

      def self.start(addr, port, storages, rttable, stats, log)
        if stats.verbose
          Roma::Event::Handler.class_eval{
            alias gets2 gets
            undef gets
        
            def gets
              ret = gets2
              @log.info("command log:#{ret.chomp}") if ret
              ret
            end
          }
        end

        EventMachine::run do
          EventMachine.start_server(addr, port, Roma::Event::Handler, storages, rttable)        
          log.info("Now accepting connections on address #{stats.address}, port #{stats.port} in the eventmachine-handler.")
        end
      end

      def self.stop
        EventMachine::stop_event_loop
      end

      def self.close_conpool(nid)
        Roma::Event::EMConPool.instance.close_same_host(nid)
        Roma::Messaging::ConPool.instance.close_same_host(nid)
      end

      def self.receiver_class
        Roma::Command::Receiver
      end

      def post_init
        @addr = Socket.unpack_sockaddr_in(get_peername)
        @log.info("Connected from #{@addr[1]}:#{@addr[0]}")
        @connected = true
        @fiber = Fiber.new { dispatcher }
      end

      def receive_data(data)
        @rbuf << data
        @fiber.resume
      rescue =>e
        @log.error("#{__FILE__}:#{__LINE__}:#{@addr[1]}:#{@addr[0]} #{e.inspect} #{$@}")
      end

      def unbind
        @connected=false
        @fiber.resume
        EventMachine::stop_event_loop if @stop_event_loop
        @log.info("Disconnected from #{@addr[1]}:#{@addr[0]}")
      rescue =>e
        @log.warn("#{__FILE__}:#{__LINE__}:#{@addr[1]}:#{@addr[0]} #{e.inspect} #{$@}")
      end

      def get_connection(ap)
        con=Roma::Event::EMConPool::instance.get_connection(ap)
        con.fiber=@fiber
        con
      end

      def return_connection(ap,con)
        Roma::Event::EMConPool.instance.return_connection(ap,con)
      end

      def dispatcher
        while(@connected) do
          next unless s=gets
          s=s.chomp.split(/ /)
          if s[0] && @receiver.ev_list.key?(s[0].downcase)
            @receiver.send(@receiver.ev_list[s[0].downcase],s)
            @lastcmd=s
          elsif s.length==0
            next
          elsif s[0]=='!!'
            @receiver.send(@receiver.ev_list[@lastcmd[0].downcase],@lastcmd)
          else
            @log.warn("command error:#{s}")
            send_data("ERROR\r\n")
            close_connection_after_writing
          end
        end
      rescue =>e
        @log.warn("#{__FILE__}:#{__LINE__}:#{@addr[1]}:#{@addr[0]} #{e} #{$@}")
        close_connection
      end

      def pop(size)
        if @rbuf.size >= size
          r = @rbuf[0..size-1]
          @rbuf = @rbuf[size..-1]
          r
        else
          nil
        end
      end

      def read_bytes(size, mult = 1)
        t=Time.now.to_i
        while(@connected) do
          d = pop(size)
          if d
            return d
          else
            remain = size - @rbuf.size
            Fiber.yield(remain)
            if Time.now.to_i - t > @timeout * mult
              @log.warn("#{__FILE__}:#{__LINE__}:#{@addr[1]}:#{@addr[0]} read_bytes time out");
              close_connection
              return nil
            end
          end
        end
        nil
      end

      def gets
        while(@connected) do
          if idx=@rbuf.index("\n")
            return pop(idx+1)
          else
            Fiber.yield(@rbuf.size)
          end
        end
        nil
      end

    end

  end
end
