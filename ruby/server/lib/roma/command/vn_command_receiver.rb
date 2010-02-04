require 'roma/async_process'

module Roma
  module Command

    module VnodeCommandReceiver

      # spushv <hash-name> <vnode-id>
      # src                             dst
      #  |  ['spushv' <hname> <vn>\r\n]->|
      #  |<-['READY'\r\n]                |
      #  |                 [<dumpdata>]->|
      #  |                       :       |
      #  |                       :       |
      #  |              [<end of dump>]->|
      #  |<-['STORED'\r\n]               |
      def ev_spushv(s)
        send_data("READY\r\n")
        @stats.run_receive_a_vnode = true
        count = rcount = 0
        @log.debug("#{__method__}:#{s.inspect} received.")
        loop {
          context_bin = read_bytes(20, 100)
          vn, last, clk, expt, klen = context_bin.unpack('NNNNN')
          break if klen == 0 # end of dump ?
          k = read_bytes(klen)
          vlen_bin = read_bytes(4, 100)
          vlen, =  vlen_bin.unpack('N')
          if vlen != 0
            v = read_bytes(vlen, 100)

            #
            # lock
            #
            if @storages[s[1]].load_stream_dump(vn, last, clk, expt, k, v)
              count += 1
#              @log.debug("#{__method__}:[#{vn} #{last} #{clk} #{expt} #{k}] was stored.")
            else
              rcount += 1
#              @log.warn("#{__method__}:[#{vn} #{last} #{clk} #{expt} #{k}] was rejected.")
            end
          else
            #
            # lock
            #
            if @storages[s[1]].load_stream_dump(vn, last, clk, expt, k, nil)
#              @log.debug("#{__method__}:[#{vn} #{last} #{clk} #{expt} #{k}] was stored.")
              count += 1
            else
              rcount += 1
#              @log.warn("#{__method__}:[#{vn} #{last} #{clk} #{expt} #{k}] was rejected.")
            end
          end
          #
          # unlock
          #
        }
        send_data("STORED\r\n")
        @log.debug("#{__method__}:#{s[2]} #{count} keys loaded. #{rcount} keys rejected.")
      rescue => e
        #
        # unlock
        #
        @log.error("#{e}\n#{$@}")
      ensure
        @stats.run_receive_a_vnode = false
      end      

      # reqpushv <vnode-id> <node-id> <is primary?>
      # src                                       dst
      #  |<-['reqpushv <vn> <nid> <p?>\r\n']         |
      #  |                           ['PUSHED'\r\n]->|
      def ev_reqpushv(s)
        if s.length!=4
          send_data("CLIENT_ERROR usage:reqpushv vnode-id node-id primary-flag(true/false)\r\n")
          return
        end

        if @stats.run_iterate_storage == true
          @log.warn("reqpushv rejected:#{s}")
          send_data("REJECTED\r\n")
          return
        end

        Roma::AsyncProcess::queue.push(Roma::AsyncMessage.new('reqpushv',[s[1],s[2],s[3]]))
        send_data("PUSHED\r\n")
      rescue =>e
        @log.error("#{e}\n#{$@}")
      end

    end # module VnodeCommandReceiver

  end # module Command
end # module Roma
