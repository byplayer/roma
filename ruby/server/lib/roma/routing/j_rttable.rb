# -*- coding: utf-8 -*-
require 'java'
require 'jar/ROMA-java-server-0.1.0-jar-with-dependencies.jar'
require 'json'

module Roma
  module Routing
    class JavaRoutingTable < Java::jp.co.rakuten.rit.roma.routing.RoutingTable
      def self.to_json rd
        JSON.generate(
          [{ :dgst_bits => rd.dgst_bits, 
             :div_bits => rd.div_bits,
             :rn => rd.rn }, # map
           rd.nodes,  # the type of nodes is array ( [] )
           rd.v_idx,  # the type of v_idx is map ( {} )
           rd.v_clk ] # the type of v_clk is map ( {} )
           )
      end

      attr :fail_cnt
      attr :fail_time
      attr :mtree
      attr_accessor :log
      attr_accessor :fail_cnt_threshold
      attr_accessor :fail_cnt_gap

      def initialize rd
        super rd
        initRoutingData(JavaRoutingTable.to_json(rd))
        @log = Roma::Logging::RLogger.instance
        @hbits = 2**dgst_bits
        @fail_cnt = Hash.new(0)
        @fail_cnt_threshold = 5
        @fail_cnt_gap = 0
        @fail_time = Time.now
        init_mtree
      end

      def get_stat ap
        pn = sn = short = lost = 0
        v_idx.each { |vn, nids|
          if nids == nil || nids.length == 0
            lost += 1
            next
          elsif nids[0] == ap
            pn += 1
          elsif nids.include?(ap)
            sn += 1
          end
          short += 1 if nids.size < rn
        }

        ret = {}
        ret['routing.redundant'] = rn
        ret['routing.nodes.length'] = nodes.size
        ret['routing.nodes'] = nodes.inspect
        ret['routing.dgst_bits'] = dgst_bits
        ret['routing.div_bits'] = div_bits
        ret['routing.vnodes.length'] = vnodes.size
        ret['routing.primary'] = pn
        ret['routing.secondary'] = sn
        ret['routing.short_vnodes'] = short
        ret['routing.lost_vnodes'] = lost
        ret['routing.fail_cnt_threshold'] = @fail_cnt_threshold
        ret['routing.fail_cnt_gap'] = @fail_cnt_gap
        ret        
      end

      def init_mtree
        @mtree = MerkleTree.new(dgst_bits, div_bits)
        v_idx.each { |vn, nids|
          @mtree.set(vn, nids)
        }
      end

      def dgst_bits
        getDgstBits
      end

      def div_bits
        getDivBits
      end

      def rn
        getRedundantNumber
      end

      def hbits
        @hbits
      end

      def v_idx
        getVirtualNodeIndexes
      end

      def v_idx_vn_clone vn
        nodes = v_idx[vn]
        ret = []
        nodes.each { |n|
          ret << n
        }
        ret
      end

      def v_clk
        getVirtualNodeClocks
      end

      def nodes # TODO
        javalist = getNodeIDs
        ret = []
        javalist.each { |l|
          ret << l
        }
        ret
      end

      def nodes= ns
        setNodeIDs nds
      end

      def vnodes
        getVirtualNodeIDs
      end

      def vnodes= vns
        setVirtualNodeIDs vns
      end

      def get_vnode_id d
        getVirtualNodeID d
      end

      def search_nodes vn
        searchNodeIDs vn
      end

      def leave nid
        nodes.delete nid
        v_idx.each { |vn, nids|
          nids.delete_if{ |nid2| nid2 == nid}
          if nids.length == 0
            @log.error("Vnode data is lost.(Vnode=#{vn})")
          end
          @mtree.set(vn,nids)
        }
        @fail_cnt.delete(nid)
      end

      def dump
        # TODO
      end

      def dump_yaml
        # TODO
      end

      def dump_json
        # TODO
      end

      def proc_failed(nid)
        t = Time.now
        if t - @fail_time > @fail_cnt_gap
          @fail_cnt[nid] += 1
          if @fail_cnt[nid] >= @fail_cnt_threshold
            leave(nid)
          end
        end
        @fail_time = t
      end

      def proc_succeed(nid)
        @fail_cnt.delete(nid)
      end

      def next_vnode(vn)
        # TODO
        n = (vn >> (dgst_bits - div_bits)) + 1
        n = 0 if n == (2**(div_bits))
        n << (dgst_bits - div_bits)
      end

      def create_nodes_from_v_idx
        # TODO
        buf_nodes={}
        v_idx.each_value{ |nids|
          nids.each{ |nid|
            buf_nodes[nid] = nid
          }
        }
        nodes= buf_nodes.values.sort
      end
    end # JavaRT
  end # module Routing
end # module Roma