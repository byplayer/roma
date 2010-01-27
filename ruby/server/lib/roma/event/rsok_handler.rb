#
# Ruby socket handler
#
require 'roma/command/receiver'
require 'roma/messaging/con_pool'
require 'roma/logging/rlogger'

module Roma
  module Event
    

    #
    # receive a first line in memcached command in a command execution thread model.
    #
    class RubySocketHandler
      
      def self.run(addr, port, storages, rttable, log)
        serv = TCPServer.new(addr, port)
                           
        event_loop = true
        while(event_loop)
          next unless select([serv],[],[],0.1)
          sock = serv.accept
      
          # command execution thread
          Thread.new {
            session = RubySocketSession.new(sock)
            log.info("Connected from #{session.addr[1]}:#{session.addr[0]}") 
            receiver = Roma::Command::Receiver.new(session, storages, rttable)
              
            while(event_loop && !session.close?)
              begin
                s = session.gets
                break if s == nil # if a closed socket by client.
                
                s = s.chomp.split(/ /)
                if s[0] && receiver.ev_list.key?(s[0].downcase)
#log.debug(s[0].downcase)
                  receiver.send(receiver.ev_list[s[0].downcase],s)
                  session.last_cmd=s
                elsif s.length==0
                  next # wait a next command
                elsif s[0]=='!!' && session.last_cmd
                  session.send_data(session.last_cmd.join(' ')+"\r\n") # command echo
                  receiver.send(receiver.ev_list[session.last_cmd[0].downcase],session.last_cmd)
                else # command error
                  log.warn("command error:#{s}")
                  session.send_data("ERROR\r\n")
                  break
                end

                # check the balse command received. 
                event_loop = false if session.stop_event_loop
              rescue IOError
                log.error("#{e.inspect}")
                break # session close for IOError.
              rescue =>e
                log.error("#{e.inspect}")
              end
            end

            session.close
            log.info("Disconnected from #{session.addr[1]}:#{session.addr[0]}")
          }
        end

      end

    end # class RubySocketHandler

    #
    # receive a first line in memcached command in the event loop model.
    #
    class RubySocketHandler2
      attr_writer :event_loop

      @@obj = nil

      def initialize(addr, port, storages, rttable, log)
        @addr = addr
        @port = port
        @storages = storages
        @rttable = rttable
        @log = log
        @session_pool = {}
        @receiver_pool = {}
        @keys = {}
        @queue = []
      end

      def self.start(addr, port, storages, rttable, stats, log)
        if stats.verbose
          Roma::Event::RubySocketSession.class_eval{
            alias gets_firstline2 gets_firstline
            undef gets_firstline

            def gets_firstline
              ret = gets_firstline2
              @log.info("command log:#{ret.chomp}") if ret
              ret
            end
          }
        end

        @@obj =  Roma::Event::RubySocketHandler2.new(addr, port, storages, rttable, log)
        @@obj.run
      end

      def self.stop
        @@obj.event_loop = false
      end

      def self.close_conpool(nid)
        Roma::Messaging::ConPool.instance.close_same_host(nid)
      end

      def self.receiver_class
        Roma::Command::Receiver
      end

      def run
        serv = TCPServer.new(@addr, @port)
        @log.info("Now accepting connections on address #{@addr}, port #{@port} in the RubySocketHandler2.")

        @reads = [serv]
        @event_loop = true
        while(@event_loop)
          sock,cmds = @queue.shift
          if sock
            unless @keys.key?(cmds[1])
              @keys[cmds[1]] = [sock,cmds]
              exec(sock,cmds)
            else
              @queue.push [sock,cmds]
            end            
          end
@log.debug("queue = #{@queue.inspect}") if @queue.length > 0
          next unless s = select(@reads,[],[],0.1)
          s[0].each{|sock|
            if sock==serv
              open_session(sock)
              next
            end
# @log.debug("selsct #{s[0].length}")
            begin
              sess = @session_pool[sock]
# @log.debug("sess #{sess}")
              cmd = sess.gets_firstline # receive a first line in memcached command.
# @log.debug("command=#{cmd}")
              unless cmd
                close_session(sock)
                next
              end

              cmds = cmd.chomp.split(/ /) # parse a command
              next if cmds.length==0

              if cmds[0]=="set"
                unless @keys.key?(cmds[1])
                  @keys[cmds[1]] = [sock,cmds]
                  @reads.delete(sock)
                  exec(sock,cmds)
                else
                  @reads.delete(sock)
                  @queue.push [sock,cmds]
                end
              else
                @reads.delete(sock)
                exec(sock,cmds)
              end

            rescue Errno::ECONNRESET, IOError => e
              @log.error("#{e.inspect}")
              close_session(sock)
            end
          }
        end
        
        @reads.each{ |s| s.close }
      end

      def open_session(sock)
        session = RubySocketSession.new(sock.accept)
        @session_pool[session.sock] = session
        receiver = Roma::Command::Receiver.new(session, @storages, @rttable)
        @receiver_pool[session.sock] = receiver
        @reads << session.sock # append a new socket
        @log.info("Connected from #{session.addr[1]}:#{session.addr[0]} reads.len=#{@reads.length}")
      end

      def close_session(sock)
        @reads.delete(sock)
        receiver = @receiver_pool[sock]
        if receiver
          @receiver_pool.delete(sock)
        end
        session = @session_pool[sock]
        if session
          @session_pool.delete(sock)
          @log.info("Disconnected from #{session.addr[1]}:#{session.addr[0]} reads.len=#{@reads.length}")
          session.close
        else
          @log.info("Disconnected #{sock}")
        end
      end

      def exec(sock, cmds)
        session = @session_pool[sock]
        receiver = @receiver_pool[sock]

        if cmds[0] && receiver.ev_list.key?(cmds[0].downcase)
          invok_cmd(receiver, session, cmds)
        elsif cmds[0]=='!!' && session.last_cmd
          session.send_data(session.last_cmd.join(' ')+"\r\n") # command echo
          invok_cmd(receiver, session, session.last_cmd)
        else # command error
          @log.warn("command error:#{cmds.inspect}")
          session.send_data("ERROR\r\n")
          close_session(session.sock)
        end

      end

      def invok_cmd(receiver, session, cmds)
# @log.debug(cmds[0].downcase)
        Thread.new{
          begin
            receiver.send(receiver.ev_list[cmds[0].downcase],cmds)
            
            @keys.delete(cmds[1]) if cmds[0]=="set"

            session.last_cmd = cmds

            # check the balse command received. 
            if session.stop_event_loop
              @event_loop = false
              close_session(session.sock)
            end

            @reads << session.sock unless session.close?
          rescue => e
            @log.error("#{e}\n#{$@}")
            close_session(session.sock)
          end
        }
      end

    end # class RubySocketHandler2

    class RubySocketSession
      attr_reader :addr
      attr_reader :sock
      attr_accessor :stop_event_loop
      attr_accessor :last_cmd
      
      def initialize(sock)
        @sock = sock
        @stop_event_loop = false
        @addr = Socket.unpack_sockaddr_in(@sock.getpeername)
        @last_cmd = nil
        @closed = false
        @cmd = nil
        @log = Roma::Logging::RLogger.instance
      end

      def get_connection(ap)
        Roma::Messaging::ConPool.instance.get_connection(ap)
      end

      def return_connection(ap,con)
        Roma::Messaging::ConPool.instance.return_connection(ap, con)
      end

      def send_data(s)
        throw IOError.new("session was closed") if @closed
        @sock.write(s)
      end

      def gets_firstline
        c = @sock.read_nonblock(1024)
        return nil if c == nil
        return '' if c.length == 0
        if @cmd
          @cmd << c
        else
          @cmd = c
        end
        ret = @cmd.slice!(/^.*\r\n/)
        return ret if ret
        ''
      end

      def gets
        if @cmd
          ret = @cmd.slice!(/^.*\r\n/)
          return ret if ret
          return @cmd + @sock.gets
        else
          nil if @closed
          @sock.gets
        end
      end

      def read_bytes(size, mult = 1)
        nil if @closed
        ret = ''
        if @cmd
          if @cmd.length >= size
            ret = @cmd[0..size-1]
            @cmd = @cmd[size..-1]
            return ret
          else
            ret = @cmd
            @cmd = nil
          end
        end

        begin
          ret << @sock.read(size - ret.length)
        end while(ret.length != size)
        ret
      end

      def close_connection_after_writing
        close
      end

      def close
        unless @closed
          @closed = true
          @sock.close
        end
      end

      def close?
        @closed
      end

    end # class RubySocketSession

  end # module Event
end # module Roam
