#
# Ruby socket handler
#
require 'roma/messaging/con_pool'

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
      
      def initialize(addr, port, storages, rttable, log)
        @addr = addr
        @port = port
        @storages = storages
        @rttable = rttable
        @log = log
        @session_pool = {}
      end

      def run
        serv = TCPServer.new(@addr, @port)
                         
        @reads = [serv]
        @event_loop = true
        while(@event_loop)
          next unless s = select(@reads,[],[],0.1)
          s[0].each{|sock|
            if sock==serv
              @reads << sock.accept # append a new socket
              next
            end
            
            begin
              cmd = sock.gets # receive a first line in memcached command.
              unless cmd
                close_connection(sock)
                next
              end

              cmds = cmd.chomp.split(/ /) # parse a command
              next if cmds.length==0

              @reads.delete(sock)
              exec(sock,cmds)

            rescue IOError
              log.error("#{e.inspect}")
              close_connection(sock)
            end
          }
        end
        
        @reads.each{ |s| s.close }
      end

      def close_connection(sock)
        @reads.delete(sock)
        session = @session_pool[sock]
        if session
          @session_pool.delete(sock)
          @log.info("Disconnected from #{session.addr[1]}:#{session.addr[0]}")
        else
          @log.info("Disconnected #{sock}")
        end
      end

      def exec(sock, cmds)
        session = nil
        if @session_pool.key?(sock)
          session = @session_pool[sock]
          @session_pool.delete(sock)
        else
          session = RubySocketSession.new(sock)
        end

        @log.info("Connected from #{session.addr[1]}:#{session.addr[0]}") 
        receiver = Roma::Command::Receiver.new(session, @storages, @rttable)


        if cmds[0] && receiver.ev_list.key?(cmds[0].downcase)
          invok_cmd(receiver, session, cmds)
        elsif cmds[0]=='!!' && session.last_cmd
          session.send_data(session.last_cmd.join(' ')+"\r\n") # command echo
          invok_cmd(receiver, session, session.last_cmd)
        else # command error
          @log.warn("command error:#{cmds.inspect}")
          session.send_data("ERROR\r\n")
          session.close
        end

      end

      def invok_cmd(receiver, session, cmds)
# @log.debug(cmds[0].downcase)
        Thread.new{
          receiver.send(receiver.ev_list[cmds[0].downcase],cmds)
          session.last_cmd = cmds

          # check the balse command received. 
          if session.stop_event_loop
            @event_loop = false
            session.close
          end

          unless session.close?
            @session_pool[session.sock] = session
            @reads << session.sock
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
      end

      def get_connection(ap)
        Roma::Messaging::ConPool.instance.get_connection(ap)
      end

      def return_connection(ap,con)
        Roma::Messaging::ConPool.instance.return_connection(ap, con)
      end

      def send_data(s)
        throw IOError.new("session was closed") unless @sock
        @sock.write(s)
      end

      def gets
        nil unless @sock
        select([@sock])
        @sock.gets
      end

      def read_bytes(size, mult = 1)
        nil unless @sock
        ret = ''
        begin
          select([@sock])
          ret << @sock.read(size - ret.length)
        end while(ret.length != size)
        ret
      end

      def close_connection_after_writing
        if @sock
          @sock.close
          @sock = nil
        end
      end

      def close
        @sock.close if @sock
      end

      def close?
        @sock == nil
      end

    end # class RubySocketSession

  end # module Event
end # module Roam
