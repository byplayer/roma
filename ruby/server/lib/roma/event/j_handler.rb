require 'java'
require 'jar/rakuten-ROMA-java-server-0.1.0-jar-with-dependencies.jar'

module Roma
  module Event
    
    class JavaEventHandler < Java::jp.co.rakuten.rit.roma.event.EventHandler
 
      def self.start(addr, port, storages, rttable, stats, log)
        if stats.verbose
          Roma::Event::JavaSocketSession.class_eval{
            undef gets_firstline

            def gets_firstline
              ret = readLine
              @log.info("command log:#{ret.chomp}") if ret
              ret
            end
          }
        end

        fact = Roma::Command::JavaReceiverFactory.new
        JavaEventHandler::run(host, port, fact)
      end

      def self.stop
#        JavaEventHandler::stop
      end

      def self.close_conpool(nid)

      end

      def self.receiver_class
#        Roma::Command::JavaReceiver
      end

    end # class JavaEventHandler < Java::jp.co.rakuten.rit.roma.event.EventHandler

    class JavaSocketSession < Java::jp.co.rakuten.rit.roma.event.Session

      def gets_firstline
        readLine()
      end

    end

  end # module Event
end # module Roam
