require 'java'
require 'jar/ROMA-java-server-0.1.0-jar-with-dependencies.jar'
require 'roma/storage/j_basic_storage'

module Roma
  module Storage
    class JavaDataStoreFactory < Java::jp.co.rakuten.rit.roma.storage.DataStoreFactory
      def initialize
        super
      end

      def initDataStore storage_path, ext_name, de_fact
        JavaTCHDataStore.new storage_path, ext_name, de_fact
      end
    end # class JavaDataStoreFactory

    class JavaTCHDataStore < Java::jp.co.rakuten.rit.roma.storage.TCHashDataStore
    end

    class JavaTCHashStorage < Roma::Storage::JavaBasicStorage
      def initialize
        super
        setDataStoreFactory JavaDataStoreFactory.new
        setDataEntryFactory JavaDataEntryFactory.new
        setLogicalClockFactory JavaLClockFactory.new
        setFileExtensionName 'tc'
      end

      def get_stat
        ret = super
        getDivisionNumber.times { |i|
          ds = getDataStoreFromIndex i
          ret["storage[#{idx}].path"] = ds.getStoragePathName
          ret["storage[#{idx}].rnum"] = ds.getRNum
          ret["storage[#{idx}].fsiz"] = ds.getFSize
        }
        ret
      end

      def opendb
        path = ''
        @storage_path.split('/').each{|p|
          if p.length==0
            path = '/'
            next
          end
          path << p
          Dir::mkdir(path) unless File.exist?(path)
          path << '/'
        }      
        super.opendb
      end
    end # class JavaTCHashStorage
  end # module Storage
end # Roma