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

      def initHandler host, port
        JavaHandler.new host, port
      end

    end

    class JavaHandler < Java::jp.co.rakuten.rit.roma.event.HandlerImpl
      @@instance = nil

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

        handler_fact = JavaHandlerFactory.new
        @@instance = handler_fact.newHandler addr, port.to_i
        @@instance.mk_evlist
        recv_fact = Roma::Command::JavaRecvFactory.new(
          storages, rttable, stats, log)
        connpool_fact = JavaConnPoolFactory.new
        conn_fact = JavaConnFactory.new
        @@instance.run recv_fact, connpool_fact, conn_fact
      end

      def mk_evlist
        Roma::Command::JavaReceiver.public_instance_methods.each{ |m|
          if m.to_s =~ /^(?:ex)?ev_(.+)$/
            addCommandMap $1, m.to_s
          end
        }
      end

      def self.stop
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
    end # class JavaHandler < Java::jp.co.rakuten.rit.roma.event.Handler
  end # module Event
end # module Roma