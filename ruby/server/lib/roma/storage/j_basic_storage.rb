require 'java'
require 'jar/ROMA-java-server-0.1.0-jar-with-dependencies.jar'
require 'digest/sha1'

module Roma
  module Storage
    class JavaDataEntryFactory < Java::jp.co.rakuten.rit.roma.storage.DataEntryFactory
      def initialize
        super
      end

      def initDataEntry key, vn, pc, lc, expt, v
        JavaDataEntry.new key, vn, pc, lc, expt, v
      end
    end

    class JavaLClockFactory < Java::jp.co.rakuten.rit.roma.storage.LogicalClockFactory
      def initialize
        super
      end
    end

    class JavaDataEntry < Java::jp.co.rakuten.rit.roma.storage.DataEntry
      def initialize key, vn, pc, lc, expt, v
        super key, vn, pc, lc, expt, v
      end

      def lclock
        getLClock.getRaw
      end

      def value
        val = getValue
        String.from_java_bytes val
      end
    end

    class JavaBasicStorage < Java::jp.co.rakuten.rit.roma.storage.BasicStorage
      attr :hdb
      attr :hdiv
      attr_reader :error_message

      attr_accessor :each_vn_dump_sleep
      attr_accessor :each_vn_dump_sleep_count
      attr_accessor :each_clean_up_sleep
      attr_accessor :logic_clock_expire

      def initialize
        super
        @each_vn_dump_sleep = 0.001
        @each_vn_dump_sleep_count = 100
        @each_clean_up_sleep = 0.01
        @logic_clock_expire = 300
      end

      def ext_name= name
        setFileExtensionName name
      end

      def ext_name
        getFileExtensionName
      end

      def storage_path= path
        setStoragePathName path
      end

      def storage_path
        getStoragePathName
      end

      def vn_list= vn_list
        setVirtualNodeIDs vn_list.to_java(:long)
      end

      def vn_list
        getVirtualNodeIDs
      end

      def divnum= divnum
        setDivisionNumber divnum
      end

      def divnum
        getDivisionNumber
      end

      def option= opt
        setOption opt
      end

      def option
        getOption
      end

      def get_stat
        ret = {}
        ret['storage.storage_path'] = File.expand_path(storage_path)
        ret['storage.divnum'] = getDivisionNumber
        ret['storage.option'] = getOption
        ret['storage.each_vn_dump_sleep'] = @each_vn_dump_sleep
        ret['storage.each_vn_dump_sleep_count'] = @each_vn_dump_sleep_count
        ret['storage.each_clean_up_sleep'] = @each_clean_up_sleep
        ret['storage.logic_clock_expire'] = @logic_clock_expire
        ret
      end

      def opendb
        open
      end

      def closedb
        close
      end

      def cmp_clk clk1, clk2
        compareLogicalClock clk1, clk2
      end
      private :cmp_clk

      def get_context vn, key, d
        e1 = createDataEntry key, vn, nil, nil, nil, nil
        e2 = super.getDataEntry e1
        return nil unless e2
        [e2.getVNodeID, e2.getPClock, e2.lclock, e2.getExpire]
      end

      def set(vn, key, d, expt, v)
        e1 = createDataEntry key, vn, nil, nil, expt, v.to_java_bytes
        e2 = execSetCommand e1
        return nil unless e2
        [e2.getVNodeID, e2.getPClock, e2.lclock, e2.getExpire, e2.value]
      end

      def get(vn, key, d)
        e1 = createDataEntry key, vn, nil, nil, nil, nil
        e2 = execGetCommand e1
        return nil unless e2
        e2.value
      end

      def get_raw(vn, key, d)
        e1 = createDataEntry key, vn, nil, nil, nil, nil
        e2 = super.getDataEntry e1
        return nil unless e2
        [e2.getVNodeID, e2.getPClock, e2.lclock, e2.getExpire, e2.value]
      end

      def clean_up(t, unit_test_flg = nil)
        n = 0
        nt = Time.now.to_i
        getDataStores.each { |ds|
          delkey = []
          ds.each{ |k, e|
            if nt > e.getExpire && t > e.getPClock
              n += 1
              #delkey << k
              ds.remove e.getKey
            end
            if unit_test_flg
              closedb
            end
            sleep @each_clean_up_sleep
          }
          #delkey.each{ |k| @hdb[i].out(k) }
        }
        n
      rescue => e
        raise NoMethodError(e.message)
      end

      def each_clean_up(t, vnhash)
        @do_clean_up = true
        nt = Time.now.to_i
        getDivisionNumber.times { |i|
          ds = getDataStoreFromIndex i
          ds.each{ |k, e|
            return unless @do_clean_up
            vn_stat = vnhash[e.getVNodeID]
            if vn_stat == :primary &&
              ((e.getExpire != 0 && nt > e.getExpire) || (e.getExpire == 0 && t > e.getPClock))
              yield k, e.getVNodeID
              if JavaDataEntry.toByteArray(ds.get(k)) == JavaDataEntry.toByteArray(e)
                ds.remove(k)
              end 
            elsif vn_stat == nil && t > e.getPClock
              yield k, e.getVNodeID
              if JavaDataEntry.toByteArray(ds.get(k)) == JavaDataEntry.toByteArray(e)
                ds.remove(k)
              end
            end
            sleep @each_clean_up_sleep
          }
        }
      end

      def stop_clean_up
         @do_clean_up = false
      end

      def load(dmp)
        n = 0
        h = Marshal.load(dmp)
        h.each_pair{ |k, v|
          # remort data
          r_vn, r_last, r_clk, r_expt = unpack_header(v)
          raise "An invalid vnode number is include.key=#{k} vn=#{r_vn}" unless @hdiv.key?(r_vn)
          local = @hdb[@hdiv[r_vn]].get(k)
          if local == nil
            n += 1
            @hdb[@hdiv[r_vn]].put(k, v)
          else
            # local data
            l_vn, l_last, l_clk, l_expt = unpack_data(local)
            if r_last - l_last < @logic_clock_expire && cmp_clk(r_clk,l_clk) <= 0
            else # remort is newer.
              n += 1
              @hdb[@hdiv[r_vn]].put(k, v)
            end
          end
          sleep @each_vn_dump_sleep
        }
        n
      end

      def load_stream_dump(vn, last, clk, expt, k, v)
        buf = @hdb[@hdiv[vn]].get(k)
        if buf
          data = unpack_header(buf)
          if last - data[1] < @logic_clock_expire && cmp_clk(clk,data[2]) <= 0
            return nil
          end
        end

        ret = [vn, last, clk, expt, v]
        if expt == 0
          return ret if @hdb[@hdiv[vn]].put(k, pack_header(*ret[0..3]))
        else
          return ret if @hdb[@hdiv[vn]].put(k, pack_data(*ret))
        end
        nil
      end

      # Returns the vnode dump.
      def dump vn
        buf = get_vnode_hash vn
        return nil if buf.length == 0
        Marshal.dump(buf) # TODO
      end

      def dump_file(path, except_vnh = nil)
        pbuf = ''
        path.split('/').each{ |p|
          pbuf << p
          begin
            Dir::mkdir(pbuf) unless File.exist?(pbuf)
          rescue
          end
          pbuf << '/'
        }
        getDivisionNumber.times { |i|
          f = open("#{path}/#{i}.dump","wb")
          each_hdb_dump(i, except_vnh){ |data| f.write(data) }
          f.close
        }
        open("#{path}/eod","w"){ |f|
          f.puts Time.now
        }
      end

      def each_vn_dump target_vn
        count = 0
        getDivisionNumber.times{ |i|
          tn =  Time.now.to_i
          getDataStoreFromIndex(i).each{ |k, e|
            vn, last, clk, expt, val = unpack_data(v)
            if e.getVNodeID != target_vn || (e.getExpire != 0 && tn > e.getExpire)
              count += 1              
              sleep @each_vn_dump_sleep if count % @each_vn_dump_sleep_count == 0
              next
            end
            if e.value
              yield [e.getVNodeID, e.getPClock, e.lclock, e.getExpire,
               e.getKey.length, e.getKey, e.value.length, e.value].pack(
                 "NNNNNa#{e.getKey.length}Na#{e.value.length}")
            else
              yield [e.getVNodeID, e.getPClock, e.lclock, e.getExpire,
               e.getKey.length, e.getKey, 0].pack("NNNNNa#{e.getKey.length}N")
            end
          }
        }
      end

      def each_hdb_dump(i, except_vnh = nil)
        count = 0
        getDataStoreFromIndex(i).each { |k, e|
          if except_vnh && except_vnh.key?(e.getVNodeID) || Time.now.to_i > e.getExpire
            count += 1
            sleep @each_vn_dump_sleep if count % @each_vn_dump_sleep_count == 0
          else
            yield [e.getVNodeID, e.getPClock, e.lclock, e.getExpire,
             e.getKey.length, e.getKey, e.value.length, e.value].pack(
               "NNNNNa#{e.getKey.length}Na#{e.value.length}")
            sleep @each_vn_dump_sleep
          end
        }
      end
      private :each_hdb_dump

      # Create vnode dump.
      def get_vnode_hash vn
        buf = {}
        count = 0
        getDataStoreFromVNodeID(vn).each { |k, e|
          count += 1
          sleep @each_vn_dump_sleep if count % @each_vn_dump_sleep_count == 0
          if e.getVNodeID == vn
            b = JavaDataEntry.toByteArray e
            buf[k] = b
          end
        }
        return buf
      end
      private :get_vnode_hash

    end # class JavaBasicStorage

  end # module Storage
end # module Roma