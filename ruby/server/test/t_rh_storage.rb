#!/usr/bin/env ruby
# -*- coding: utf-8 -*-

begin
  base_path
rescue => e
  require 'test/unit'
  require 'pathname'
  base_path = Pathname(__FILE__).dirname.parent.parent.expand_path
  $LOAD_PATH.unshift("#{base_path}/server/lib")
end

require 'test/roma-test-storage-utils'

class RubyHashStorageTest2 < Test::Unit::TestCase
  include BasicStorageTestUtil

  def initialize(arg)
    super(arg)
    @ndat = 1000
  end

  def setup
    require 'roma/storage/rh_storage'
    @st = Roma::Storage::RubyHashStorage.new
    @st.vn_list = [0]
    @st.opendb
  end

  def teardown
  end

  def test_cmp_clk
    (0x001E00000..0x002000000).each{|clk|
      assert_equal(0, @st.send(:cmp_clk,clk, clk) )
    }
    (0x001E00000..0x002000000).each{|clk|
      assert_operator(0,:>, @st.send(:cmp_clk,clk-1, clk) )
      assert_operator(0,:<, @st.send(:cmp_clk,clk, clk-1) )
    }
    (0x001E00000..0x002000000).each{|clk|
      assert_operator(0,:<, @st.send(:cmp_clk,clk+1, clk) )
      assert_operator(0,:>, @st.send(:cmp_clk,clk, clk+1) )
    }
    # t1=0 t2=0 clk2=0b0000...
    clk1=0x00000010
    clk2=0x00000000
    assert_operator(0,:<, @st.send(:cmp_clk,clk1, clk2) )
    assert_operator(0,:>, @st.send(:cmp_clk,clk2, clk1) )
    # t1=0 t2=1 clk2=0b0010...
    clk2=0x20000000
    assert_operator(0,:>, @st.send(:cmp_clk,clk1, clk2) )
    assert_operator(0,:<, @st.send(:cmp_clk,clk2, clk1) )
    # t1=0 t2=2 clk2=0b0100...
    clk2=0x40000000
    assert_operator(0, :>, @st.send(:cmp_clk,clk1, clk2) )
    assert_operator(0, :<, @st.send(:cmp_clk,clk2, clk1) )
    # t1=0 t2=3 clk2=0b0110...
    clk2=0x60000000
    assert_operator(0,:>, @st.send(:cmp_clk,clk1, clk2) )
    assert_operator(0,:<, @st.send(:cmp_clk,clk2, clk1) )
    # t1=0 t2=4 clk2=0b1000...
    clk2=0x80000000
    assert_operator(0,:>, @st.send(:cmp_clk,clk1, clk2) )
    assert_operator(0,:<, @st.send(:cmp_clk,clk2, clk1) )

    # t1=0 t2=5 clk2=0b1010...
    clk2=0xa0000000
    assert_operator(0,:<, @st.send(:cmp_clk,clk1, clk2) )
    assert_operator(0,:>, @st.send(:cmp_clk,clk2, clk1) )
    # t1=0 t2=6 clk2=0b1100...
    clk2=0xc0000000
    assert_operator(0,:<, @st.send(:cmp_clk,clk1, clk2))
    assert_operator(0,:>, @st.send(:cmp_clk,clk2, clk1))
    # t1=0 t2=7 clk2=0b1110...
    clk2=0xe0000000
    assert_operator(0,:<, @st.send(:cmp_clk,clk1, clk2) )
    assert_operator(0,:>, @st.send(:cmp_clk,clk2, clk1) )
  end

end
