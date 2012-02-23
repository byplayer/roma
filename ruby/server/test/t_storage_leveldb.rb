#!/usr/bin/env ruby

require 'roma/storage/leveldb_storage'
require File.expand_path('t_storage', File.dirname(__FILE__))

class LevelDBStorageTest < TCStorageTest
  OPTION_TEST_DIR='storage_test_option'

  def setup
    rmtestdir('storage_test')
    @st = Roma::Storage::LevelDBStorage.new
    @st.vn_list = [0,1,2,3,4,5,6,7,8,9]
    @st.storage_path = 'storage_test'
    @st.opendb
  end

  def teardown
    super

    if @option_st
      @option_st.closedb
    end
    rmtestdir(OPTION_TEST_DIR)
  end

=begin
  options[ :paranoid_checks ]
    Default: false

options[ :write_buffer_size ]
    Default: 4MB

options[ :max_open_files ]

    Default: 1000

options[ :block_cache_size ]
    Default: nil

options[ :block_size ]
    Default: 4K

options[ :block_restart_interval ]
    Default: 16
options[ :compression ]

    LevelDB::CompressionType::SnappyCompression or LevelDB::CompressionType::NoCompression.

    Default: LevelDB::CompressionType::SnappyCompression


read option
write option
=end
  def test_option_paranoid_checks_defaults
    rmtestdir(OPTION_TEST_DIR)
    @option_st = Roma::Storage::LevelDBStorage.new
    @option_st.vn_list = [0,1,2,3,4,5,6,7,8,9]
    @option_st.storage_path = OPTION_TEST_DIR
    @option_st.opendb

    @option_st.hdb.each do |db|
      assert_equal(db.options.paranoid_checks, false)
    end
  end

  def test_option_paranoid_checks
    rmtestdir(OPTION_TEST_DIR)
    @option_st = Roma::Storage::LevelDBStorage.new
    @option_st.vn_list = [0,1,2,3,4,5,6,7,8,9]
    @option_st.storage_path = OPTION_TEST_DIR
    @option_st.option = "paranoid_checks=true"
    @option_st.opendb

    @option_st.hdb.each do |db|
      assert_equal(db.options.paranoid_checks, true)
    end
  end
end

