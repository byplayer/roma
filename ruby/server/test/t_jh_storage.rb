#!/usr/bin/env ruby
# -*- coding: utf-8 -*-

begin
  base_path
rescue => e
  require 'pathname'
  base_path = Pathname(__FILE__).dirname.parent.parent.expand_path
  $LOAD_PATH.unshift("#{base_path}/server/lib")
end

require 'test/unit'
require 'test/roma-test-storage-utils'

class JavaHashMapStorageTest < Test::Unit::TestCase
  if defined? JRUBY_VERSION
    require 'java'
    require 'jar/ROMA-java-server-0.1.0-jar-with-dependencies.jar'
    require 'roma/storage/j_jh_storage'

    include BasicStorageTestUtil
  end

  def initialize(arg)
    super(arg)
    @ndat = 1000
  end

  def setup
    if defined? JRUBY_VERSION
      @st = Roma::Storage::JavaHashMapStorage.new
      @st.vn_list = [0]
      @st.opendb
    end
  end

  def teardown
  end

  def test_dummy
  end
end