#
# Ruby socket handler
#
# require 'socket'
require 'roma/messaging/con_pool'

module Roma
  module Event
    
    class RubySocketHandler
      
      def self.run(addr, port, storages, rttable, log)
        serv = TCPServer.new(port)
                           
        event_loop = true
        while(event_loop)
          begin
            sleep 0.01
            sok, sockaddr = serv.accept_nonblock
          
            if(sok)
              log.debug("accept!!")
              Thread.new {
                session = RubySocketSession.new(sok)
                receiver = Roma::Command::Receiver.new(session, storages, rttable)

                while(event_loop)
                  begin
                    select([sok])
                    s = sok.gets
                    s = s.chomp.split(/ /)
                    if s[0] && receiver.ev_list.key?(s[0].downcase)
                      log.debug(s[0].downcase)
                      receiver.send(receiver.ev_list[s[0].downcase],s)
                      #                    @lastcmd=s
                    elsif s.length==0
                      next
                      #                  elsif s[0]=='!!'
                      #                    receiver.send(receiver.ev_list[@lastcmd[0].downcase],@lastcmd)
                    else
                      log.warn("command error:#{s}")
                      sok.write("ERROR\r\n")
                      sok.close
                    end

                    event_loop = false if session.stop_event_loop
                  rescue =>e
                    log.error("#{e}")
                  end
                end
              }
            end
          rescue Errno::EAGAIN
          end
        end

      end

      def self.dispatcher
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



    end

    class RubySocketSession
      
      attr_accessor :stop_event_loop
      
      def initialize(sok)
        @sok = sok
        @stop_event_loop = false
      end

      def get_connection(ap)
        Roma::Messaging::ConPool.instance.get_connection(ap)
      end

      def return_connection(ap,con)
        Roma::Messaging::ConPool.instance.return_connection(ap, con)
      end

      def send_data(s)
        @sok.write(s)
      end

      def gets
        select([@sok])
        @sok.gets
      end

      def read_bytes(size, mult = 1)
        ret = ''
        begin
          select([@sok])
          ret << @sok.readpartial(size - ret.length)
        end while(ret.length != size)
        ret
      end

      def close_connection_after_writing
        @sok.close
      end


    end

  end # Event
end # Roam
