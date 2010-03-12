#!/usr/bin/env ruby
# -*- coding: utf-8 -*-

begin
  base_path
rescue => e
  require 'pathname'
  base_path = Pathname(__FILE__).dirname.parent.parent.expand_path
  $LOAD_PATH.unshift("#{base_path}/server/lib")
end

require 'roma/storage/rh_storage'
require 'test/roma-test-storage-utils'
require 'test/unit'

class RubyHashStorageTest < Test::Unit::TestCase
  include BasicStorageTestUtil

  def initialize(arg)
    super(arg)
    @ndat = 1000
  end

  def setup
    @st = Roma::Storage::RubyHashStorage.new
    @st.vn_list = [0]
    @st.opendb
  end

  def teardown
  end

end
