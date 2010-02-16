require 'java'
require 'jar/ROMA-java-server-0.1.0-jar-with-dependencies.jar'
require 'roma/storage/j_basic_storage'

module Roma
  module Storage
    class JavaDataStoreFactory < Java::jp.co.rakuten.rit.roma.storage.DataStoreFactory
      def initialize
        super
      end

      def initDataStore storage_path, ext_name
        JavaHashMapDataStore.new storage_path, ext_name
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
    
    class JavaHashMapDataStore < Java::jp.co.rakuten.rit.roma.storage.HashMapDataStore
    end

    class JavaHashStorage < Roma::Storage::JavaBasicStorage
      def initialize
        super
        setDataStoreFactory JavaDataStoreFactory.new
        setDataEntryFactory JavaDataEntryFactory.new
        setLogicalClockFactory JavaLClockFactory.new
      end
    end # class JavaHashStorage
  end # module Storage
end # module Roma
