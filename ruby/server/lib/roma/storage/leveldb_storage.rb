require 'leveldb'
require 'roma/storage/basic_storage'

module Roma
  module Storage

    class LevelDBStorage < BasicStorage
      include LevelDB

      class LevelDB::DB
        alias get_org get
        def get key
          begin
            ret = get_org key
          rescue Exception => ecode
            raise StorageException, errmsg(ecode)
          end
          ret
        end

        alias put_org put
        def put key, value
          begin
            ret = put_org key, value
          rescue Exception => ecode
            raise StorageException, errmsg(ecode) unless ret
          end
          ret
        end

        alias out_org delete
        def out key
          begin
            ret = out_org key
          rescue Exception => ecode
            raise StorageException, errmsg(ecode)
          end
          ret
        end

        alias rnum_org size
        def rnum
          begin
            ret = rnum_org
          rescue Exception => ecode
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

        if @fname_lock
          if File.exist?(@fname_lock)
            File.unlink(@fname_lock) if @fname_lock
          end
          @fname_lock = nil
        end
      end

      private
      def parse_options
        return {} unless @option

        opt = {}
        buf = @option.split('#')
        buf.each do |equ|
          if /(\S+)\s*=\s*(\S+)/ =~ equ
            key = $1.to_sym
            val = $2

            if key == :compression
              if val == "NoCompression"
                opt[key] = LevelDB::CompressionType::NoCompression
              elsif val == "SnappyCompression"
                opt[key] = LevelDB::CompressionType::SnappyCompression
              end
            elsif val == "true"
              opt[key] = true
            elsif val == "false"
              opt[key] = false
            elsif val =~ /^[0-9]+$/
              opt[key] = val.to_i
            else
              opt[key] = val
            end
          else
            raise RuntimeError.new("Option string parse error.")
          end
        end

        opt
      end

      def open_db(fname)
        begin
          @parsed_option = parse_options
          LevelDB::DB.new(fname, @parsed_option)
        rescue => ecode
          raise RuntimeError, "ldb open error: #{ecode.to_s}", ecode.backtrace
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
