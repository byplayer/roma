require 'roma/stats'
require 'roma/command/sys_command_receiver'
require 'roma/command/bg_command_receiver'
require 'roma/command/rt_command_receiver'
require 'roma/command/st_command_receiver'
require 'roma/command/util_command_receiver'
require 'roma/command/mh_command_receiver'

module Roma
  module Command

    class Receiver
      @@ev_list={}

      include SystemCommandReceiver
      include BackgroundCommandReceiver
      include RoutingCommandReceiver
      include StorageCommandReceiver
      include UtilCommandReceiver
      include MultiHashCommandReceiver

      def initialize(session, storages, rttable)
        @storages = storages
        @rttable = rttable
        @stats = Roma::Stats.instance
        @nid = @stats.ap_str
        @defhash = 'roma'
        @log = Roma::Logging::RLogger.instance
        @session = session

        unless has_event?
          public_methods.each{|m|
            if m.to_s.start_with?('ev_')
              add_event(m.to_s[3..-1],m)
            end
          }
        end
      end

      def stop_event_loop
        @session.stop_event_loop = true
      end

      def get_connection(ap)
        @session.get_connection(ap)
      end

      def return_connection(ap,con)
        @session.return_connection(ap,con)
      end

      def send_data(s)
        @session.send_data(s)
      end

      def gets
        @session.gets
      end

      def read_bytes(size, mult = 1)
        @session.read_bytes(size, mult)
      end

      def close_connection_after_writing
        @session.close_connection_after_writing
      end

      def has_event?
        @@ev_list.length!=0
      end

      def add_event(c,m)
        @@ev_list[c]=m
      end

      def ev_list
        @@ev_list
      end

    end # class Receiver < Roma::Event::Handler

  end # module Command
end # module Roma
