require 'roma/stats'
require 'roma/command/sys_command_receiver'
require 'roma/command/bg_command_receiver'
require 'roma/command/rt_command_receiver'
require 'roma/command/st_command_receiver'
require 'roma/command/util_command_receiver'
require 'roma/command/mh_command_receiver'

module Roma
  module Command
    class JavaRecvFactory < Java::jp.co.rakuten.rit.roma.event.ReceiverFactory
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

      def initReceiver sess
        Roma::Command::JavaReceiver.new sess
      end

      def postReceiverInit recv
        recv.postInit
        recv.storages = @storages
        recv.rttable = @rttable
        recv.stats = @stats
        recv.nid = @stats.ap_str
        recv.defhash = 'roma'
        recv.log = @log
      end
    end

    class JavaReceiver < Java::jp.co.rakuten.rit.roma.event.Receiver
      @@ev_list={}

      include Roma::Command::SystemCommandReceiver
      include Roma::Command::BackgroundCommandReceiver
      include Roma::Command::RoutingCommandReceiver
      include Roma::Command::StorageCommandReceiver
      include Roma::Command::UtilCommandReceiver
      include Roma::Command::MultiHashCommandReceiver

      attr_accessor :storages
      attr_accessor :rttable
      attr_accessor :stats
      attr_accessor :nid
      attr_accessor :defhash
      attr_accessor :log

      def postInit
        unless has_event?
          public_methods.each{ |m|
            if m.to_s.start_with?('ev_')
              add_event(m.to_s[3..-1], m)
            end
          }
        end
      end

      def has_event?
        @@ev_list.length != 0
      end

      def add_event c, m
        @@ev_list[c] = m
      end

      def ev_list
        @@ev_list
      end

      def execCommand cmds
        s = []
        cmds.each{ |cmd|
          s << cmd
        }
        puts "command name: #{ev_list[s[0].downcase]}"
        self.send(ev_list[s[0].downcase], s)
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
        conn = getConnection ap
        conn.extend(Roma::Event::ConnectionUtil)
        conn
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