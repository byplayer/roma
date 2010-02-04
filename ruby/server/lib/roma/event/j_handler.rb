require 'java'
require 'jar/ROMA-java-server-0.1.0-jar-with-dependencies.jar'
require 'roma/command/j_receiver'

module Roma
  module Event
    class JavaConnPool < Java::jp.co.rakuten.rit.roma.event.ConnectionPool
      def initialize size
        super size
      end
      
      def get_connection ap
        conn = get ap
        conn.extend(Roma::Event::ConnectionUtil)
        conn
      end
      
      def return_connection ap, conn
        put ap, conn
      end
      
      def delete_connection ap
        delete ap
      end
      
      def close_all
        close_all
      end
      
      def close_same_host ap
        delete ap
      end
    end

    class JavaConnPoolFactory < Java::jp.co.rakuten.rit.roma.event.ConnectionPoolFactory
      def initialize
        super
      end

      def initConnectionPool size
        JavaConnPool.new size
      end
    end

    module ConnectionUtil
      def read_bytes(len)
        bytes = readBytes len
        String.from_java_bytes bytes
      end

#      def gets; gets; end

      def write s
        writeString s
      end
    end

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

        recv_fact = Roma::Command::JavaRecvFactory.new storages, rttable, stats, log
        conn_pool_fact = JavaConnPoolFactory.new
        JavaHandler::run addr, port.to_i, recv_fact, conn_pool_fact
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