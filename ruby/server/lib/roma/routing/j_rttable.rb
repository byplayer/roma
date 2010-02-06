require 'json'

module Roma
  module Routing
    class JavaRoutingTable
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

      attr_accessor :row_rttable

      def initialize rd, fname
        rt_fact = JavaRTFactory.new
        rd_json = JavaRoutingTable.to_json rd
        @row_rttable = rt_fact.newRoutingTable rd_json.to_s, fname
      end
    end # class JavaRoutingTable

    class JavaRTFactory < Java::jp.co.rakuten.rit.roma.routing.RoutingTableFactory
      def initialize
        super
      end

      def initRoutingTable rd, fname
        JavaRT.new rd, fname
      end
    end # JavaRoutingTableFactory

    class JavaRT < Java::jp.co.rakuten.rit.roma.routing.RoutingTable
      def initialize rd, fname
        super rd, fname
      end

    end # JavaRT
  end # module Routing
end # module Roma