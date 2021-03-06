require 'json'

module Roma
  module CommandPlugin

    module PluginGui
      include ::Roma::CommandPlugin

      # get_routing_history
      def ev_get_routing_history(s)
        routing_path  = get_config_stat["config.RTTABLE_PATH"]
        contents = ""
        Dir.glob("#{routing_path}/*").each{|fname|
          contents << File.read(fname) if !FileTest::directory?(fname) && fname =~ /#{@stats.ap_str}\.route*/
        }
        routing_list = contents.scan(/[-\.a-zA-Z\d]+_[\d]+/).uniq.sort
        routing_list.each{|routing|
          send_data("#{routing}\r\n")
        }
        send_data("END\r\n")
      end

      # gather_logs [start_date(YYYY-MM-DDThh:mm:ss)] <end_date(YYYY-MM-DDThh:mm:ss)>
      def ev_gather_logs(s)
        if s.length < 2 || s.length > 3
          return send_data("CLIENT_ERROR number of arguments (#{s.length-1} for 2-3)\r\n")
        end

        start_date = s[1]
        end_date = s[2]
        end_date ||= 'current'

        if @stats.gui_run_gather_logs
          return send_data("CLIENT_ERROR gathering process is already going\r\n")
        end

        begin
          @stats.gui_run_gather_logs = true
          Roma::AsyncProcess::queue.push(Roma::AsyncMessage.new('start_get_logs', [start_date, end_date]))

          send_data("STARTED\r\n")
        rescue
          @stats.gui_run_gather_logs = false
          @rttable.logs = []
          send_data("CLIENT_ERROR\r\n")
        end
      end

      # show_logs
      def ev_show_logs(s)
        if @stats.gui_run_gather_logs
          send_data("Not finished gathering\r\n")
        else
          @rttable.logs.each_with_index{|log, index|
            send_data("#{log}\r\n")
            sleep @stats.stream_show_wait_param if index % 10 == 0
          }
          send_data("END\r\n")
          @rttable.logs.clear
        end
      end

    end # end of module PluginGui
  end # end of module CommandPlugin
end # end of modlue Roma
