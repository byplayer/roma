require 'java'
require 'jar/ROMA-java-server-0.1.0-jar-with-dependencies.jar'
require 'roma/storage/j_basic_storage'

module Roma
  module Storage
    class JavaDataStoreFactory < Java::jp.co.rakuten.rit.roma.storage.DataStoreFactory
      def initialize
        super
      end

      def initDataStore storage_path, ext_name, options, de_fact, lc_fact
        JavaHashMapDataStore.new storage_path, ext_name, options, de_fact, lc_fact
      end
    end # class JavaDataStoreFactory

    class JavaHashMapDataStore < Java::jp.co.rakuten.rit.roma.storage.HashMapDataStore
    end # class JavaHashMapDataStore

    def opendb
      super.opendb
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
