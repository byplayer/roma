require 'java'
require 'jar/ROMA-java-server-0.1.0-jar-with-dependencies.jar'
require 'roma/event/j_connpool'
require 'roma/command/j_receiver'

module Roma
  module Event
    class JavaHandler < Java::jp.co.rakuten.rit.roma.event.Handler
      def self.start(addr, port, storages, rttable, stats, log)
        if stats.verbose
          Roma::Event::JavaSocketSession.class_eval {
            undef gets_firstline

            def gets_firstline
              ret = readLine
              @log.info("command log:#{ret.chomp}") if ret
              ret
            end
          }
        end

        ev_list = JavaHandler.mk_evlist
        recv_fact = Roma::Command::JavaRecvFactory.new storages, rttable, stats, log, ev_list
        conn_pool_fact = JavaConnPoolFactory.new
        conn_fact = JavaConnFactory.new
        JavaHandler::run addr, port.to_i, recv_fact, conn_pool_fact, conn_fact
      end

      def self.mk_evlist
        ev_list = {}
        Roma::Command::JavaReceiver.public_instance_methods.each{|m|
          ev_list[$1] = m if m.to_s =~ /^(?:ex)?ev_(.+)$/
        }
        ev_list
      end

      def self.stop
        JavaHandler::stop
      end

      def self.close_conpool(nid)

      end
      
      def self.receiver_class
        Roma::Command::JavaReceiver
      end
      
      def self.con_pool
        JavaHandler::getConnectionPool
      end
    end # class JavaHandler < Java::jp.co.rakuten.rit.roma.event.Handler
  end # module Event
end # module Roma