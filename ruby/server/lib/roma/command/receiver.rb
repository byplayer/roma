require 'roma/stats'
require 'roma/version'
require 'roma/messaging/con_pool'
require 'roma/command/bg_command_receiver'
require 'roma/command/rt_command_receiver'
require 'roma/command/st_command_receiver'
require 'roma/command/util_command_receiver'
require 'roma/command/mh_command_receiver'

module Roma
  module Command

    class Receiver
      @@ev_list={}

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

      # balse [reason]
      def ev_balse(s)
        send_data("Are you sure?(yes/no)\r\n")
        if gets != "yes\r\n"
          close_connection_after_writing
          return
        end

        if s.length == 2
          @log.info("Receive a balse #{s[1]}")
        else
          @log.info("Receive a balse command.")
        end
        @rttable.enabled_failover = false
        res = broadcast_cmd("rbalse\r\n")
        send_data("#{res.inspect}\r\n")
        close_connection_after_writing
        stop_event_loop
      end

      # rbalse [reason]
      def ev_rbalse(s)
        if s.length == 2
          @log.info("Receive a rbalse #{s[1]}")
        else
          @log.info("Receive a rbalse command.")
        end
        @rttable.enabled_failover = false
        send_data("BYE\r\n")
        close_connection_after_writing
        stop_event_loop
      end

      # version
      def ev_version(s)
        send_data("VERSION #{Roma::VERSION}\r\n")
      end

      # quit
      def ev_quit(s)
        close_connection_after_writing
      end

      def ev_whoami(s)
        send_data("#{@stats.name}\r\n")
      end

      # stats [regexp]
      def ev_stats(s); ev_stat(s); end

      # stat [regexp]
      def ev_stat(s)
        regexp = s[1] if s.length == 2
        h = {}
        h['version'] = Roma::VERSION
        send_stat_result(nil,h,regexp)
        send_stat_result(nil,Roma::Config.get_stat,regexp)
        send_stat_result(nil,@stats.get_stat,regexp)
        @storages.each{|hname,st|
          send_stat_result("storages[#{hname}].",st.get_stat,regexp)
        }
        send_stat_result(nil,$roma.wb_get_stat,regexp)
        send_stat_result(nil,@rttable.get_stat(@stats.ap_str),regexp)
        send_data("END\r\n")
      end

      def send_stat_result(prefix,h,regexp = nil)
        h.each{|k,v|         
          if prefix
            key = "#{prefix}#{k}"
          else
            key = "#{k}"
          end
          if regexp
            send_data("#{key} #{v}\r\n") if key =~ /#{regexp}/
          else
            send_data("#{key} #{v}\r\n")
          end
        }
      end
      private :send_stat_result

      # writebehind_rotate [hash_name]
      def ev_writebehind_rotate(s)
        if s.length < 2
          return send_data("CLIENT_ERROR number of arguments (0 for 1)\r\n")
        end
        res = broadcast_cmd("rwritebehind_rotate #{s[1]}\r\n")

        if $roma.wb_rotate(s[1])
          res[@stats.ap_str] = "ROTATED"
        else
          res[@stats.ap_str] = "NOT_OPEND"
        end
        send_data("#{res}\r\n")
      end

      # rwritebehind_rotate [hash_name]
      def ev_rwritebehind_rotate(s)
        if s.length < 2
          return send_data("CLIENT_ERROR number of arguments (0 for 1)\r\n")
        end
        if $roma.wb_rotate(s[1])
          send_data("ROTATED\r\n")
        else
          send_data("NOT_OPEND\r\n")
        end
      end

      # writebehind_get_path [hash_name]
      def ev_writebehind_get_path(s)
        if s.length < 2
          return send_data("CLIENT_ERROR number of arguments (0 for 1)\r\n")
        end
        res = broadcast_cmd("rwritebehind_get_path #{s[1]}\r\n")
        
        ret = $roma.wb_get_path(s[1])
        res[@stats.ap_str] = ret

        send_data("#{res}\r\n")
      end

      # rwritebehind_get_path [hash_name]
      def ev_rwritebehind_get_path(s)
        if s.length < 2
          return send_data("CLIENT_ERROR number of arguments (0 for 1)\r\n")
        end
        
        ret = $roma.wb_get_path(s[1])
        send_data("#{ret}\r\n")
      end
      
      # writebehind_get_current_file [hash_name]
      def ev_writebehind_get_current_file(s)
        if s.length < 2
          return send_data("CLIENT_ERROR number of arguments (0 for 1)\r\n")
        end
        res = broadcast_cmd("rwritebehind_get_current_file #{s[1]}\r\n")

        ret = $roma.wb_get_current_file_path(s[1])
        if ret
          res[@stats.ap_str] = ret
        else
          res[@stats.ap_str] = "NOT_OPEND"
        end
        send_data("#{res}\r\n")
      end

      # rwritebehind_get_current_file [hash_name]
      def ev_rwritebehind_get_current_file(s)
        if s.length < 2
          return send_data("CLIENT_ERROR number of arguments (0 for 1)\r\n")
        end
        ret = $roma.wb_get_current_file_path(s[1])
        if ret
          send_data("#{ret}\r\n")
        else
          send_data("NOT_OPEND\r\n")
        end
      end

      # dcnice command is setting priority for a data-copy thread.
      # a niceness of 1 is the highest priority and 5 is the lowest priority.
      # dcnice <priority:1 to 5>
      def ev_dcnice(s)
        if s.length < 2
          return send_data("CLIENT_ERROR number of arguments (0 for 1)\r\n")
        end
        res = broadcast_cmd("rdcnice #{s[1]}\r\n")
        res[@stats.ap_str] = dcnice(s[1].to_i)
        send_data("#{res}\r\n")
      end

      def ev_rdcnice(s)
        if s.length < 2
          return send_data("CLIENT_ERROR number of arguments (0 for 1)\r\n")
        end

        send_data("#{dcnice(s[1].to_i)}\r\n")
      end
      
      # set_log_level [ 'debug' | 'info' | 'warn' | 'error' ] 
      def ev_set_log_level(s)
        if s.length < 2
          return send_data("CLIENT_ERROR number of arguments (0 for 1)\r\n")
        end

        case s[1].downcase
        when 'debug'
          @log.level = Roma::Logging::RLogger::Severity::DEBUG
        when 'info'
          @log.level = Roma::Logging::RLogger::Severity::INFO
        when 'warn'
          @log.level = Roma::Logging::RLogger::Severity::WARN
        when 'error'
          @log.level = Roma::Logging::RLogger::Severity::ERROR
        else
          return send_data("CLIENT_ERROR no match log-level string\r\n")
        end
        send_data("STORED\r\n")
      end

      private 

      def dcnice(p)
        case(p)
        when 1 # highest priority
          @stats.stream_copy_wait_param = 0.001
          @storages.each_value{|st|
            st.each_vn_dump_sleep = 0.001
            st.each_vn_dump_sleep_count = 1000
          }
        when 2
          @stats.stream_copy_wait_param = 0.005
          @storages.each_value{|st|
            st.each_vn_dump_sleep = 0.005
            st.each_vn_dump_sleep_count = 100
          }
        when 3 # default priority
          @stats.stream_copy_wait_param = 0.01
          @storages.each_value{|st|
            st.each_vn_dump_sleep = 0.001
            st.each_vn_dump_sleep_count = 10
          }
        when 4
          @stats.stream_copy_wait_param = 0.01
          @storages.each_value{|st|
            st.each_vn_dump_sleep = 0.005
            st.each_vn_dump_sleep_count = 10
          }
        when 5 # lowest priority
          @stats.stream_copy_wait_param = 0.01
          @storages.each_value{|st|
            st.each_vn_dump_sleep = 0.01
            st.each_vn_dump_sleep_count = 10
          }
        else
          return "CLIENT_ERROR You sholud input a priority from 1 to 5."
        end
        "STORED"
      end
    end # class Receiver < Roma::Event::Handler

  end # module Command
end # module Roma
