require 'roma/async_process'
require 'roma/command/vn_command_receiver'

module Roma
  module Command

    module BackgroundCommandReceiver
      include VnodeCommandReceiver

      def ev_balance(s)
        res = broadcast_cmd("rbalance\r\n")
        if @stats.run_recover==false && 
            @stats.run_acquire_vnodes == false &&
            @rttable.vnode_balance(@stats.ap_str)==:less
          Roma::AsyncProcess::queue.push(Roma::AsyncMessage.new('start_acquire_vnodes_process'))
          res[@stats.ap_str] = 'STARTED'
        else
          res[@stats.ap_str] = 'SERVER_ERROR Not unbalance or Balance/Recover/Sync process is already running.'
        end
        send_data("#{res}\r\n")
      end

      def ev_rbalance(s)
        if @stats.run_recover==false && 
            @stats.run_acquire_vnodes == false &&
            @rttable.vnode_balance(@stats.ap_str)==:less
          Roma::AsyncProcess::queue.push(Roma::AsyncMessage.new('start_acquire_vnodes_process'))
          send_data("STARTED\r\n")
        else
          send_data("SERVER_ERROR Not unbalance or Balance/Recover/Sync process is already running.\r\n")
        end
      end

      def ev_release(s)
        if @stats.run_recover==false && 
            @stats.run_acquire_vnodes == false &&
            @stats.run_release == false &&
            @stats.run_iterate_storage == false
          Roma::AsyncProcess::queue.push(Roma::AsyncMessage.new('start_release_process'))
          send_data("STARTED\r\n")
        else
          send_data("SERVER_ERROR Release/Balance/Recover/Sync process is already running.\r\n")
        end
      end

      # recover [-r|-s]
      def ev_recover(s)
        option = s[1] if s.length == 2
        if @rttable.can_i_recover?
          cmd = "rrecover"
          cmd << " #{option}" if option
          res = broadcast_cmd("#{cmd}\r\n")
          unless @stats.run_recover
            Roma::AsyncProcess::queue.push(Roma::AsyncMessage.new('start_recover_process',[option]))
            res[@nid] = "STARTED"
          else
            res[@nid] = "SERVER_ERROR Recover/Sync process is already running."
          end
          send_data("#{res}\r\n")
        else
          send_data("SERVER_ERROR nodes num < redundant num\r\n")
        end
      end

      # rrecover [-r|-s]
      def ev_rrecover(s)
        option = s[1] if s.length == 2
        if @rttable.can_i_recover?
          unless @stats.run_recover
            Roma::AsyncProcess::queue.push(Roma::AsyncMessage.new('start_recover_process',[option]))
            send_data("STARTED\r\n")
          else
            send_data("SERVER_ERROR Recover process is already running.\r\n")
          end
        else
          send_data("SERVER_ERROR nodes num < redundant num\r\n")
        end
      end

      # sync <hname>
      def ev_sync(s)
        res = nil
        if s.length==1
          res = broadcast_cmd("rsync\r\n")
        else
          res = broadcast_cmd("rsync #{s[1]}\r\n")
        end
        unless @stats.run_recover
          if s.length==1
            Roma::AsyncProcess::queue.push(Roma::AsyncMessage.new('start_sync_process',@storages.keys))
          else
            Roma::AsyncProcess::queue.push(Roma::AsyncMessage.new('start_sync_process',[s[1]]))
          end
          res[@nid] = "STARTED"
        else
          res[@nid] = "SERVER_ERROR Recover/Sync process is already running."
        end
        send_data("#{res}\r\n")
      rescue => e
        @log.error("#{e}\n#{$@}")
      end

      # rsync <hname>
      def ev_rsync(s)
        unless @stats.run_recover
          if s.length==1
            Roma::AsyncProcess::queue.push(Roma::AsyncMessage.new('start_sync_process',@storages.keys))
          else
            Roma::AsyncProcess::queue.push(Roma::AsyncMessage.new('start_sync_process',[s[1]]))
          end
          send_data("STARTED\r\n")
        else
          send_data("SERVER_ERROR Recover/Sync process is already running.\r\n")
        end
      rescue => e
        @log.error("#{e}\n#{$@}")
      end

      # dumpfile <key> <path>
      def ev_dumpfile(s)
        if s.length != 3
          send_data("CLIENT_ERROR usage:dumpfile <key> <path>\r\n")
          return
        end

        res = broadcast_cmd("rdumpfile #{s[1]} #{s[2]}\r\n")
        Roma::AsyncProcess::queue.push(Roma::AsyncMessage.new('start_dumpfile_process',[s[1],s[2],:dumpfile]))
        path = Roma::Config::STORAGE_DUMP_PATH + '/' + s[2]
        res[@nid] = "STARTED #{path}/#{@nid}"
        send_data("#{res}\r\n")
      rescue => e
        @log.error("#{e}\n#{$@}")
      end

      # rdumpfile <key> <path>
      def ev_rdumpfile(s)
        if s.length != 3
          send_data("CLIENT_ERROR usage:rdumpfile <key> <path>\r\n")
          return
        end
        Roma::AsyncProcess::queue.push(Roma::AsyncMessage.new('start_dumpfile_process',[s[1],s[2],:rdumpfile]))
        path = Roma::Config::STORAGE_DUMP_PATH + '/' + s[2]
        send_data("STARTED #{path}/#{@nid}\r\n")
      rescue => e
        @log.error("#{e}\n#{$@}")
      end

    end # module BackgroundCommandReceiver

  end # module Command
end # module Roma
