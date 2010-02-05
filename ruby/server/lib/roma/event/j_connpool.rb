module Roma
  module Event
    class JavaConnPoolFactory < Java::jp.co.rakuten.rit.roma.event.ConnectionPoolFactory
      def initialize
        super
      end

      def initConnectionPool size
        JavaConnPool.new size
      end
    end

    class JavaConnPool < Java::jp.co.rakuten.rit.roma.event.ConnectionPool
      def initialize size
        super size
      end

      def get_connection ap
         get ap
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

    class JavaConnFactory < Java::jp.co.rakuten.rit.roma.event.ConnectionFactory
      def initialize
        super
      end

      def initConnection sock
        JavaConn.new sock
      end
    end

    class JavaConn < Java::jp.co.rakuten.rit.roma.event.Connection
      def initialize sock
        super sock
      end

      def read_bytes len
        bytes = readBytes len
        String.from_java_bytes bytes
      end

#      def gets; gets; end

      def write s
        writeString s
      end
    end
  end # module Event
end # module Roma