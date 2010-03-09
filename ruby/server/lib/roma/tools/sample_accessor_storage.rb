#!/usr/bin/env ruby
# -*- coding: utf-8 -*-

path =  File.dirname(File.expand_path($PROGRAM_NAME))
$LOAD_PATH << path + "/../../../lib"
$LOAD_PATH << path  + "/../../../../commons/lib"

require 'optparse'
require 'roma/config'
require 'roma/stats'
require 'roma/routing/routing_data'

module Roma
  module Storage
    class SampleAccessor
      attr :stats
      attr :storages
      attr :rttable

      def initialize(argv = nil)
        @stats = Roma::Stats.instance
        options(argv)
        initialize_rttable
        initialize_storages
      end

      def options(argv)
        opts = OptionParser.new
        opts.banner="usage:#{File.basename($0)} [options] address"
        opts.on_tail("-h", "--help", "Show this message.") {
          puts opts; exit
        }
        @size_of_value = 100
        opts.on("-l", "--len [n]", "Size of value.The default value is 100.") { |v|
          @size_of_value = v.to_i if v.to_i > 0
        }
        @num_of_keys = 10
        opts.on("-n", "--num [n]",
                "Number of keys by multiply of 10000.The default value is 10 for 10 * 10000 = 100000.") { |v|
          @num_of_keys = v.to_i
        }
        @stats.port = Roma::Config::DEFAULT_PORT.to_s
        opts.on("-p", "--port [PORT]") { |v| @stats.port = v }
        opts.parse!(argv)
        raise OptionParser::ParseError.new if argv.length < 1
        @stats.address = argv[0]
        unless @stats.port =~ /^\d+$/
          raise OptionParser::ParseError.new('Port number is not numeric.')
        end
      rescue OptionParser::ParseError => e
        $stderr.puts e.message
        $stderr.puts opts.help
        exit 1
      end

      def initialize_storages
        @storages = {}
        nid = @stats.ap_str
        st = Roma::Config::STORAGE_CLASS.new
        st.storage_path = "#{Roma::Config::STORAGE_PATH}/#{nid}/roma"
        st.vn_list = @rttable.vnodes
        st.divnum = Roma::Config::STORAGE_DIVNUM
        st.option = Roma::Config::STORAGE_OPTION
        @storages[nid] = st
      end

      def initialize_rttable
        raise "#{@stats.ap_str}.route not found." unless File::exist?("#{@stats.ap_str}.route")
        rd = Roma::Routing::RoutingData::load("#{@stats.ap_str}.route")
        raise "It failed in loading the routing table data." unless rd
        @rttable = Roma::Config::RTTABLE_CLASS.new(rd,"#{@stats.ap_str}.route")
      end

      def start
        @storages.each{ |hashname, st|
          st.opendb
        }
        access_storage
        @storages.each{ |hashname, st|
          st.closedb
        }
      end

      def rand_str(n)
        buf = []
        n.times{|i|
          buf << rand(0x7e - 0x20)+0x20
        }
        buf.pack('c*')
      end

      def rand_str(n)
        buf = []
        n.times{|i|
          buf << rand(0x7e - 0x20)+0x20
        }
        buf.pack('c*')
      end

      def access_storage
        t1 = DateTime.now
        dummy_str = []
        dummy_str[0] = rand_str(@size_of_value - (@size_of_value / 5))
        dummy_str[1] = rand_str(@size_of_value - (@size_of_value / 10))
        dummy_str[2] = rand_str(@size_of_value)
        dummy_str[3] = rand_str(@size_of_value + (@size_of_value / 10))
        dummy_str[4] = rand_str(@size_of_value + (@size_of_value / 5))
        pn = @num_of_keys * 10000
        mod1 = pn / 100
        cnt = 0
        pn.times{ |i|
          if i % mod1 == 0
            printf('.')
            if (cnt += 1) == 10
              puts (i+mod1)
              cnt = 0
            end
          end
          tt0 = DateTime.now
          k = i.to_s
          d = Digest::SHA1.hexdigest(k).hex % @rttable.hbits
          vn = @rttable.get_vnode_id(d)
          nodes = @rttable.search_nodes_for_write(vn)
          v = dummy_str[rand(5)] + "::" + i.to_s
#          tt1 = DateTime.now
#          puts "time1: #{(tt1 - tt0).to_f}"
          if nodes.include?(@stats.ap_str)
            res = @storages[@stats.ap_str].set(vn, k, d, 0x7fffffff, v)
#            puts "set k=#{k}, #{res}, nid=#{nid}"
          end
#          tt2 = DateTime.now
#          puts "time1: #{(tt2 - tt1).to_f}"
        }
        t2 = DateTime.now
        sec = (t2 - t1).to_f * 86400
        printf("%.4f (sec) %.1f (qps)\n", sec, pn / sec)
      end
    end
  end
end

accessor = Roma::Storage::SampleAccessor.new(ARGV)
accessor.start