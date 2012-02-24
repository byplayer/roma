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

  def test_option_write_buffer_size_defaults
    rmtestdir(OPTION_TEST_DIR)
    @option_st = Roma::Storage::LevelDBStorage.new
    @option_st.vn_list = [0,1,2,3,4,5,6,7,8,9]
    @option_st.storage_path = OPTION_TEST_DIR
    @option_st.opendb

    @option_st.hdb.each do |db|
      assert_equal(db.options.write_buffer_size, 4 * 1024 * 1024)
    end
  end

  def test_option_write_buffer_size
    rmtestdir(OPTION_TEST_DIR)
    @option_st = Roma::Storage::LevelDBStorage.new
    @option_st.vn_list = [0,1,2,3,4,5,6,7,8,9]
    @option_st.storage_path = OPTION_TEST_DIR
    @option_st.option = "write_buffer_size=#{8*1024*1024}"
    @option_st.opendb

    @option_st.hdb.each do |db|
      assert_equal(db.options.write_buffer_size, 8 * 1024 * 1024)
    end
  end

  def test_option_max_open_files_defaults
    rmtestdir(OPTION_TEST_DIR)
    @option_st = Roma::Storage::LevelDBStorage.new
    @option_st.vn_list = [0,1,2,3,4,5,6,7,8,9]
    @option_st.storage_path = OPTION_TEST_DIR
    @option_st.opendb

    @option_st.hdb.each do |db|
      assert_equal(db.options.max_open_files, 1000)
    end
  end

  def test_option_max_open_files
    rmtestdir(OPTION_TEST_DIR)
    @option_st = Roma::Storage::LevelDBStorage.new
    @option_st.vn_list = [0,1,2,3,4,5,6,7,8,9]
    @option_st.storage_path = OPTION_TEST_DIR
    @option_st.option = "max_open_files=2500"
    @option_st.opendb

    @option_st.hdb.each do |db|
      assert_equal(db.options.max_open_files, 2500)
    end
  end

  def test_block_cache_size_default
    rmtestdir(OPTION_TEST_DIR)
    @option_st = Roma::Storage::LevelDBStorage.new
    @option_st.vn_list = [0,1,2,3,4,5,6,7,8,9]
    @option_st.storage_path = OPTION_TEST_DIR
    @option_st.opendb

    @option_st.hdb.each do |db|
      assert_equal(db.options.block_cache_size, nil)
    end
  end

  def test_block_cache_size
    rmtestdir(OPTION_TEST_DIR)
    @option_st = Roma::Storage::LevelDBStorage.new
    @option_st.vn_list = [0,1,2,3,4,5,6,7,8,9]
    @option_st.storage_path = OPTION_TEST_DIR
    @option_st.option = "block_cache_size=#{16 * 1024 * 1024}"
    @option_st.opendb

    @option_st.hdb.each do |db|
      assert_equal(db.options.block_cache_size, 16 * 1024 * 1024)
    end
  end

  def test_block_size_default
    rmtestdir(OPTION_TEST_DIR)
    @option_st = Roma::Storage::LevelDBStorage.new
    @option_st.vn_list = [0,1,2,3,4,5,6,7,8,9]
    @option_st.storage_path = OPTION_TEST_DIR
    @option_st.opendb

    @option_st.hdb.each do |db|
      assert_equal(db.options.block_size, 4 * 1024)
    end
  end

  def test_block_size
    rmtestdir(OPTION_TEST_DIR)
    @option_st = Roma::Storage::LevelDBStorage.new
    @option_st.vn_list = [0,1,2,3,4,5,6,7,8,9]
    @option_st.storage_path = OPTION_TEST_DIR
    @option_st.option = "block_size=#{1 * 1024}"
    @option_st.opendb

    @option_st.hdb.each do |db|
      assert_equal(db.options.block_size, 1 * 1024)
    end
  end

  def test_block_restart_interval_default
    rmtestdir(OPTION_TEST_DIR)
    @option_st = Roma::Storage::LevelDBStorage.new
    @option_st.vn_list = [0,1,2,3,4,5,6,7,8,9]
    @option_st.storage_path = OPTION_TEST_DIR
    @option_st.opendb

    @option_st.hdb.each do |db|
      assert_equal(db.options.block_restart_interval, 16)
    end
  end

  def test_block_restart_interval
    rmtestdir(OPTION_TEST_DIR)
    @option_st = Roma::Storage::LevelDBStorage.new
    @option_st.vn_list = [0,1,2,3,4,5,6,7,8,9]
    @option_st.storage_path = OPTION_TEST_DIR
    @option_st.option = "block_restart_interval=32"
    @option_st.opendb

    @option_st.hdb.each do |db|
      assert_equal(db.options.block_restart_interval, 32)
    end
  end

  def test_compression_default
    rmtestdir(OPTION_TEST_DIR)
    @option_st = Roma::Storage::LevelDBStorage.new
    @option_st.vn_list = [0,1,2,3,4,5,6,7,8,9]
    @option_st.storage_path = OPTION_TEST_DIR
    @option_st.opendb

    @option_st.hdb.each do |db|
      assert_equal(db.options.compression, LevelDB::CompressionType::SnappyCompression)
    end
  end

  def test_compression
    rmtestdir(OPTION_TEST_DIR)
    @option_st = Roma::Storage::LevelDBStorage.new
    @option_st.vn_list = [0,1,2,3,4,5,6,7,8,9]
    @option_st.storage_path = OPTION_TEST_DIR
    @option_st.option = "compression=NoCompression"
    @option_st.opendb

    @option_st.hdb.each do |db|
      assert_equal(db.options.compression, LevelDB::CompressionType::NoCompression)
    end
  end

  def test_option_multi
    rmtestdir(OPTION_TEST_DIR)
    @option_st = Roma::Storage::LevelDBStorage.new
    @option_st.vn_list = [0,1,2,3,4,5,6,7,8,9]
    @option_st.storage_path = OPTION_TEST_DIR
    @option_st.option = "block_size=#{1 * 1024}#block_restart_interval=32#" +
      "block_cache_size=#{16 * 1024 * 1024}"
    @option_st.opendb

    @option_st.hdb.each do |db|
      assert_equal(db.options.block_size, 1 * 1024)
      assert_equal(db.options.block_restart_interval, 32)
      assert_equal(db.options.block_cache_size, 16 * 1024 * 1024)
    end
  end
end

