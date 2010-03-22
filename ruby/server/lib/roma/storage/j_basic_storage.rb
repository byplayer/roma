require 'java'
require 'jar/ROMA-java-server-0.1.0-jar-with-dependencies.jar'
require 'digest/sha1'

module Roma
  module Storage
    class JavaLClockFactory < Java::jp.co.rakuten.rit.roma.storage.LogicalClockFactory
      def initialize
        super
      end

      def initLogicalClock time
        JavaLamportClock.new time
      end
    end

    class JavaLamportClock < Java::jp.co.rakuten.rit.roma.storage.LamportClock
      def initialize time
        super time
      end
    end

    class JavaDataEntryFactory < Java::jp.co.rakuten.rit.roma.storage.DataEntryFactory
      def initialize
        super
      end

      def initDataEntry key, vn, pc, lc, expt, v
        JavaDataEntry.new key, vn, pc, lc, expt, v
      end
    end

    class JavaDataEntry < Java::jp.co.rakuten.rit.roma.storage.DataEntry
      def initialize key, vn, pc, lc, expt, v
        super key, vn, pc, lc, expt, v
      end
      
      def key
        getKey
      end
      
      def vn
        getVNodeID
      end
      
      def pclock
        getPClock
      end

      def lclock
        getLClock.getRaw
      end

      def expire
        getExpire
      end

      def val
        v = getData
        return nil unless v
        String.from_java_bytes v
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
        setStorageNameAndPath path
      end

      def storage_path
        getStoragePathName
      end

      def vn_list= vn_list
        list = []
        vn_list.each { |vn|
          list << vn
        }
        setVirtualNodeIDs list.to_java(:long)
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

      def logical_clock_expire
        getLogicalClockExpireTime
      end
      
      def get_stat
        ret = {}
        ret['storage.storage_path'] = File.expand_path(storage_path)
        ret['storage.divnum'] = getDivisionNumber
        ret['storage.option'] = getOption
        ret['storage.each_vn_dump_sleep'] = @each_vn_dump_sleep
        ret['storage.each_vn_dump_sleep_count'] = @each_vn_dump_sleep_count
        ret['storage.each_clean_up_sleep'] = @each_clean_up_sleep
        ret['storage.logic_clock_expire'] = logic_clock_expire
        ret
      end

      def cmp_clk clk1, clk2
        compareLogicalClock clk1, clk2
      end
      private :cmp_clk

      def opendb
        openDataStores
      end

      def closedb
        closeDataStores
      end

      def get_context vn, key, d
        e1 = createDataEntry key, vn, nil, nil, nil, nil
        e2 = getDataEntry e1
        return nil unless e2
        [e2.vn, e2.pclock, e2.lclock, e2.expire]
      end

      def cas(vn, key, d, exp, v)
        e1 = createDataEntry key, vn, nil, nil, exp, v.to_java_bytes
        e2 = execCasCommand e1
        return nil unless e2
        [e2.vn, e2.pclock, e2.lclock, e2.expire, e2.val]
      end

      def set(vn, key, d, exp, v)
        e1 = createDataEntry key, vn, nil, nil, exp, v.to_java_bytes
        e2 = execSetCommand e1
        return nil unless e2
        [e2.vn, e2.pclock, e2.lclock, e2.expire, e2.val]
      end

      def rset(vn, key, d, lc, exp, v)
        e1 = createDataEntry key, vn, nil, lc, exp, v.to_java_bytes
        e2 = execRSetCommand e1
        return nil unless e2
        [e2.vn, e2.pclock, e2.lclock, e2.expire, e2.val]
      end

      def add(vn, key, d, exp, v)
        e1 = createDataEntry key, vn, nil, nil, exp, v.to_java_bytes
        e2 = execAddCommand e1
        return nil unless e2
        [e2.vn, e2.pclock, e2.lclock, e2.expire, e2.val]
      end

      def replace(vn, key, d, exp, v)
        e1 = createDataEntry key, vn, nil, nil, exp, v.to_java_bytes
        e2 = execReplaceCommand e1
        return nil unless e2
        [e2.vn, e2.pclock, e2.lclock, e2.expire, e2.val]
      end

      def append(vn, key, d, exp, v)
        e1 = createDataEntry key, vn, nil, nil, exp, v.to_java_bytes
        e2 = execAppendCommand e1
        return nil unless e2
        [e2.vn, e2.pclock, e2.lclock, e2.expire, e2.val]
      end

      def prepend(vn, key, d, exp, v)
        e1 = createDataEntry key, vn, nil, nil, exp, v.to_java_bytes
        e2 = execPrependCommand e1
        return nil unless e2
        [e2.vn, e2.pclock, e2.lclock, e2.expire, e2.val]
      end

      def get(vn, key, d)
        e1 = createDataEntry key, vn, nil, nil, nil, nil
        e2 = execGetCommand e1
        return nil unless e2
        e2.val
      end

      def get_raw(vn, key, d)
        e1 = createDataEntry key, vn, nil, nil, nil, nil
        e2 = getDataEntry e1
        return nil unless e2
        [e2.vn, e2.pclock, e2.lclock, e2.expire, e2.val]
      end

      def delete(vn, key, d)
        e1 = createDataEntry key, vn, nil, nil, nil, nil
        e2 = execDeleteCommand e1
        return nil unless e2
        return [] unless e2.val
        if e2.val.length != 0
          [e2.vn, e2.pclock, e2.lclock, e2.expire, e2.val]
        else
          [e2.vn, e2.pclock, e2.lclock, e2.expire, nil]
        end
      end

      def rdelete(vn, key, d, lclock)
        e1 = createDataEntry key, vn, nil, lclock, nil, nil
        e2 = execRDeleteCommand e1
        return nil unless e2
        [e2.vn, e2.pclock, e2.lclock, e2.expire]
      end

      def out(vn, key, d)
        e1 = createDataEntry key, vn, nil, nil, nil, nil
        e2 = execOutCommand e1
      end

      def incr(vn, key, d, v)
        e1 = createDataEntry key, vn, nil, nil, nil, v.to_s.to_java_bytes
        e2 = execIncrCommand e1
        return nil unless e2
        [e2.vn, e2.pclock, e2.lclock, e2.expire, e2.val]
      end

      def decr(vn, key, d, v)
        e1 = createDataEntry key, vn, nil, nil, nil, v.to_s.to_java_bytes
        e2 = execDecrCommand e1
        return nil unless e2
        [e2.vn, e2.pclock, e2.lclock, e2.expire, e2.val]
      end

      def each_clean_up(t, vnhash)
        @do_clean_up = true
        nt = Time.now.to_i
        getDataStores.each { |ds|
          deletelist = []
          ds.each{ |k, e|
            return unless @do_clean_up
            vn_stat = vnhash[e.vn]
            if vn_stat == :primary && ((e.expire != 0 && nt > e.expire) || (e.expire == 0 && t > e.pclock))
              yield k, e.vn
              r = String.from_java_bytes(JavaDataEntry.toByteArray(ds.get(k)))
              l = String.from_java_bytes(JavaDataEntry.toByteArray(e))
              if r == l
                deletelist << k
              end 
            elsif vn_stat == nil && t > e.pclock
              yield k, e.vn
              r = String.from_java_bytes(JavaDataEntry.toByteArray(ds.get(k)))
              l = String.from_java_bytes(JavaDataEntry.toByteArray(e))
              if r == l
                deletelist << k
              end
            end
            sleep @each_clean_up_sleep
          }
          deletelist.each { |k|
            ds.out k
          }
        }
      end

      def stop_clean_up
         @do_clean_up = false
      end

      def load_stream_dump(vn, last, clk, expt, key, v)
        e1 = createDataEntry key, vn, nil, nil, nil, nil
        e2 = execGetCommand e1
        if e2
          if last - e2.pclock < @logic_clock_expire && cmp_clk(clk, e2.lclock) <= 0
            return nil
          end
        end

        e3 = createDataEntry key, vn, last, clk, expt, v.to_java_bytes
        if expt == 0
          if execSetCommand e3
            return [vn, last, clk, expt, v]
          end
        else
          if execSetCommand e3
            return [vn, last, clk, expt, v]
          end
        end
        nil
      end

      # Return the vnode dump.
      def dump vn
        buf = get_vnode_hash vn
        return nil if buf.length == 0
        Marshal.dump(buf)
      end

      def dump_file path, except_vnh = nil
        pbuf = ''
        path.split('/').each{ |p|
          pbuf << p
          begin
            Dir::mkdir(pbuf) unless File.exist?(pbuf)
          rescue
          end
          pbuf << '/'
        }
        divnum.times { |i|
          f = open("#{path}/#{i}.dump", "wb")
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
          tn = Time.now.to_i
          getDataStoreFromIndex(i).each{ |k, e|
            if e.getVNodeID != target_vn || (e.getExpire != 0 && tn > e.getExpire)
              count += 1
              sleep @each_vn_dump_sleep if count % @each_vn_dump_sleep_count == 0
              next
            end
            if e.val
              yield [e.vn, e.pclock, e.lclock, e.expire,
               e.key.length, e.key, e.val.length, e.val].pack(
                 "NNNNNa#{e.key.length}Na#{e.val.length}")
            else
              yield [e.vn, e.pclock, e.lclock, e.expire,
               e.key.length, e.key, 0].pack("NNNNNa#{e.getKey.length}N")
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
            yield [e.vn, e.pclock, e.lclock, e.expire,
             e.key.length, e.key, e.val.length, e.val].pack(
               "NNNNNa#{e.getKey.length}Na#{e.val.length}")
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
          if e.vn == vn
            buf[k] = String.from_java_bytes(JavaDataEntry.toByteArray(e))
          end
        }
        return buf
      end
      private :get_vnode_hash

    end # class JavaBasicStorage

  end # module Storage
end # module Roma