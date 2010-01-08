#
# Ruby socket handler
#
require 'roma/messaging/con_pool'

module Roma
  module Event
    
    class RubySocketHandler
      
      def self.run(addr, port, storages, rttable, log)
        serv = TCPServer.new(addr, port)
                           
        event_loop = true
        while(event_loop)
          next unless select([serv],[],[],0.1)
          sok = serv.accept
          
          Thread.new {
            session = RubySocketSession.new(sok)
            log.info("Connected from #{session.addr[1]}:#{session.addr[0]}") 
            receiver = Roma::Command::Receiver.new(session, storages, rttable)
              
            lastcmd = nil
            while(event_loop && !session.close?)
              begin
                s = session.gets
                break if s == nil # if a closed socket by client.
                
                s = s.chomp.split(/ /)
                if s[0] && receiver.ev_list.key?(s[0].downcase)
#log.debug(s[0].downcase)
                  receiver.send(receiver.ev_list[s[0].downcase],s)
                  lastcmd=s
                elsif s.length==0
                  next # wait a next command
                elsif s[0]=='!!' && lastcmd
                  session.send_data(lastcmd.join(' ')+"\r\n") # command echo
                  receiver.send(receiver.ev_list[lastcmd[0].downcase],lastcmd)
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

    class RubySocketSession
      attr_reader :addr
      attr_reader :sok
      attr_accessor :stop_event_loop
      
      def initialize(sok)
        @sok = sok
        @stop_event_loop = false
        @addr = Socket.unpack_sockaddr_in(@sok.getpeername)
      end

      def get_connection(ap)
        Roma::Messaging::ConPool.instance.get_connection(ap)
      end

      def return_connection(ap,con)
        Roma::Messaging::ConPool.instance.return_connection(ap, con)
      end

      def send_data(s)
        throw IOError.new("session was closed") unless @sok
        @sok.write(s)
      end

      def gets
        nil unless @sok
        select([@sok])
        @sok.gets
      end

      def read_bytes(size, mult = 1)
        nil unless @sok
        ret = ''
        begin
          select([@sok])
          ret << @sok.read(size - ret.length)
        end while(ret.length != size)
        ret
      end

      def close_connection_after_writing
        if @sok
          @sok.close
          @sok = nil
        end
      end

      def close
        @sok.close if @sok
      end

      def close?
        @sok == nil
      end

    end # class RubySocketSession

  end # module Event
end # module Roam
