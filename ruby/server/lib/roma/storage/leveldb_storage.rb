require 'leveldb'
require 'roma/storage/basic_storage'

module Roma
  module Storage

    class LeveldbStorage < BasicStorage
      include LevelDB
      
      class LevelDB::DB
        alias get_org get
        def get key
          begin
            ret = get_org key
          rescue => ecode
            raise StorageException, errmsg(ecode)
          end
          ret
        end
        
        alias put_org put
        def put key, value
          begin
            ret = put_org key, value
          rescue => ecode
            raise StorageException, errmsg(ecode) unless ret
          end
          ret
        end
        
        alias out_org delete
        def out key
          begin
            ret = out_org key
          rescue => ecode
            raise StorageException, errmsg(ecode)
          end
          ret
        end

        alias rnum_org size
        def rnum
          begin
            ret = rnum_org
          rescue => ecode
            raise StorageException, errmsg(ecode)
          end
          ret
        end
      end

      def initialize
        super
        @ext_name = 'ldb'
      end

      def get_stat
        ret = super
        @hdb.each_with_index{|hdb,idx|
          ret["storage[#{idx}].rnum"] = hdb.size
        }
        ret
      end

      def opendb
        create_div_hash
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

        @fname_lock = "#{@storage_path}/lock"
        if File.exist?(@fname_lock)
          raise RuntimeError.new("Lock file already exists.")
        end
        open(@fname_lock,"w"){}

        @divnum.times{ |i|
          @hdb[i] = open_db("#{@storage_path}/#{i}.#{@ext_name}")
        }
      end

      def closedb
        stop_clean_up
        buf = @hdb; @hdb = []
        buf.each{ |hdb| close_db(hdb) }

        File.unlink(@fname_lock) if @fname_lock
        @fname_lock = nil
      end

      protected

      def set_options(hdb)
        #prop = parse_options

        #prop.each_key{|k|
        #  unless /^(bnum|apow|fpow|opts|xmsiz|rcnum|dfunit)$/ =~ k
        #    raise RuntimeError.new("Syntax error, unexpected option #{k}")
        #  end
        #}
        
        #opts = 0
        #if prop.key?('opts')
        #  opts |= HDB::TLARGE if prop['opts'].include?('l')
        #  opts |= HDB::TDEFLATE if prop['opts'].include?('d')
        #  opts |= HDB::TBZIP if prop['opts'].include?('b')
        #  opts |= HDB::TTCBS if prop['opts'].include?('t')
        #end

        #hdb.tune(prop['bnum'].to_i,prop['apow'].to_i,prop['fpow'].to_i,opts)

        #hdb.setxmsiz(prop['xmsiz'].to_i) if prop.key?('xmsiz')
        #hdb.setcache(prop['rcnum'].to_i) if prop.key?('rcnum')
        #hdb.setdfunit(prop['dfunit'].to_i) if prop.key?('dfunit')
      end

      private

      def parse_options
        #return Hash.new(-1) unless @option
        #buf = @option.split('#')
        #prop = Hash.new(-1)
        #buf.each{|equ|
        #  if /(\S+)\s*=\s*(\S+)/ =~ equ
        #    prop[$1] = $2
        #  else
        #    raise RuntimeError.new("Option string parse error.")
        #  end
        #}
        #prop
      end

      def open_db(fname)
        #begin
        #LevelDB::DB::new fname
        #rescue => ecode
        #  raise RuntimeError.new("ldb open error #{ecode}")
        #end

        #set_options(hdb)
        
        #if !hdb.open(fname, HDB::OWRITER | HDB::OCREAT | HDB::ONOLCK)
        #  ecode = hdb.ecode
        #  raise RuntimeError.new("ldb open error")
        #end

        #hdb
        begin
          LevelDB::DB::new fname
        rescue => ecode
          raise RuntimeError.new("ldb open error")
        end
      end

      def close_db(hdb)
        if !hdb.close
          #ecode = hdb.ecode
          raise RuntimeError.new("ldb close error")
        end
      end

    end # class LeveldbStorage

  end # module Storage
end # module Roma