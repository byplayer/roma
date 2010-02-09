require 'roma/stats'
require 'roma/command/sys_command_receiver'
require 'roma/command/bg_command_receiver'
require 'roma/command/rt_command_receiver'
require 'roma/command/st_command_receiver'
require 'roma/command/util_command_receiver'
require 'roma/command/mh_command_receiver'

module Roma
  module Command
    class JavaRecvFactory < Java::jp.co.rakuten.rit.roma.command.ReceiverFactory
      attr_accessor :storages
      attr_accessor :rttable
      attr_accessor :stats
      attr_accessor :log

      def initialize(storages, rttable, stats, log)
        super()
        @storages = storages
        @rttable = rttable
        @stats = stats
        @log = log
      end

      def initReceiver handler, sess
        JavaReceiver.new handler, sess
      end

      def postReceiverInit recv
        recv.storages = @storages
        recv.rttable = @rttable
        recv.stats = @stats
        recv.nid = @stats.ap_str
        recv.defhash = 'roma'
        recv.log = @log
      end
    end

    class JavaReceiver < Java::jp.co.rakuten.rit.roma.command.Receiver

      include SystemCommandReceiver
      include BackgroundCommandReceiver
      include RoutingCommandReceiver
      include StorageCommandReceiver
      include UtilCommandReceiver
      include MultiHashCommandReceiver

      attr_accessor :storages
      attr_accessor :rttable
      attr_accessor :stats
      attr_accessor :nid
      attr_accessor :defhash
      attr_accessor :log

      def execCommand cmds
        s = []
        cmds.each{ |cmd|
          s << cmd
        }
        puts "command name: #{s[0].downcase}"
#        puts "method name: #{getCommandName(s[0].downcase)}"
        begin
          self.send(getCommandName(s[0].downcase), s)
        rescue => e
          @log.error("#{e}\n#{$@}")
          close_connection_after_writing
        end
      end

      def send_data s
        writeString s
      end

      def read_bytes len
        bytes = readBytes len
        String.from_java_bytes bytes
      end

      def stop_event_loop
        stopEventLoop
      end

      def get_connection ap
        getConnection ap
      end

      def return_connection ap, conn
        putConnection ap, conn
      end

      def close_connection_after_writing
        sess = getSession
        sess.close
      end
    end
  end # Roma::Command
end # Roma
