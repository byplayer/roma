
module Roma
  module Command

    module UtilCommandReceiver

      def send_cmd(nid, cmd)
        con = get_connection(nid)
        con.write(cmd)
        res = con.gets
        if res
          res.chomp!
          @rttable.proc_succeed(nid)
          return_connection(nid, con)
        else
          @rttable.proc_failed(nid)
        end
        res
      rescue => e
        @rttable.proc_failed(nid)
        @log.error("#{e}\n#{$@}")
        nil
      end

      def broadcast_cmd(cmd)
        res={}
        @rttable.nodes.each{|nid|
          res[nid] = send_cmd(nid,cmd) if nid != @stats.ap_str
        }
        res
      end

    end # module UtilCommandReceiver

  end # module Command
end # module Roma
