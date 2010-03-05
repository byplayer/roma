require 'java'
require 'jar/ROMA-java-server-0.1.0-jar-with-dependencies.jar'
require 'roma/command/j_connpool'
require 'roma/command/j_receiver'

module Roma
  module Event
    class JavaHandlerFactory < Java::jp.co.rakuten.rit.roma.event.HandlerFactory
      def initialize
        super
      end

      def initHandler host, port, connpool_fact, conn_fact
        JavaHandler.new host, port, connpool_fact, conn_fact
      end

    end

    class JavaHandler < Java::jp.co.rakuten.rit.roma.event.HandlerImpl
      @@instance = nil

      def self.init addr, port
        connpool_fact = JavaConnPoolFactory.new
        conn_fact = JavaConnFactory.new
        handler_fact = JavaHandlerFactory.new
        @@instance = handler_fact.newHandler addr, port.to_i, connpool_fact, conn_fact
      end

      def self.start(storages, rttable, stats, log)
        recv_fact = Roma::Command::JavaRecvFactory.new(
          storages, rttable, stats, log)
        @@instance.mk_evlist
        @@instance.run recv_fact
      end

      def self.stop
        @@instance.stop
      end

      def self.con_pool
        @@instance.getConnectionPool
      end

      def self.close_conpool(nid)
        con_pool().close_same_host(nid)
      end

      def self.receiver_class
        Roma::Command::JavaReceiver
      end

      def mk_evlist
        JavaHandler::receiver_class.public_instance_methods.each{ |m|
          if m.to_s =~ /^(?:ex)?ev_(.+)$/
            addCommandMap $1, m.to_s
          end
        }
      end

    end # class JavaHandler < Java::jp.co.rakuten.rit.roma.event.Handler
  end # module Event
end # module Roma