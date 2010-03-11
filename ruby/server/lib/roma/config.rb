require 'roma/storage/rh_storage'
require 'roma/routing/cb_rttable'

if defined? JRUBY_VERSION
  #require 'roma/storage/j_tc_storage'
  require 'roma/storage/j_jh_storage'
  require 'roma/event/j_handler'
  require 'roma/routing/j_cb_rttable'
else
  require 'roma/event/handler'
  require 'roma/event/rsok_handler'
end

module Roma

  module Config
    DEFAULT_PORT = 12000
    DEFAULT_NAME = 'ROMA'

    # :no_action | :auto_assign | :shutdown
    DEFAULT_LOST_ACTION = :auto_assign

    # log setting
    LOG_SHIFT_AGE = 10
    LOG_SHIFT_SIZE = 1024 * 1024 * 10
    LOG_PATH = '.'
    # :debug | :info | :warn | :error
    LOG_LEVEL = :debug

    # network event handler setting
    # HANDLER_CLASS = Roma::Event::Handler
    # HANDLER_CLASS = Roma::Event::RubySocketHandler
    HANDLER_CLASS = Roma::Event::JavaHandler

    # routing setting
    RTTABLE_CLASS = Roma::Routing::JavaCBRoutingTable
    # RTTABLE_CLASS = Roma::Routing::ChurnbasedRoutingTable
    RTTABLE_PATH = '.'

    # storage setting
    STORAGE_CLASS = Roma::Storage::JavaHashMapStorage
    # STORAGE_CLASS = Roma::Storage::RubyHashStorage
    STORAGE_DIVNUM = 10
    STORAGE_PATH = '.'
    STORAGE_DUMP_PATH = '/tmp'
    STORAGE_OPTION = ''

    # 5 days ago
    STORAGE_DELMARK_EXPTIME = 60 * 60 * 24 * 5

    # data copy setting
    DATACOPY_STREAM_COPY_WAIT_PARAM = 0.0001

    # plugin setting
    PLUGIN_FILES = ['plugin_storage.rb']

    # write-behind setting
    WRITEBEHIND_PATH = './wb'
    WRITEBEHIND_SHIFT_SIZE = 1024 * 1024 * 10

    # redundant setting
    REDUNDANT_ZREDUNDANT_SIZE = 0

    def self.get_stat
      ret = {}
      ret['config.DEFAULT_LOST_ACTION'] = DEFAULT_LOST_ACTION
      ret['config.LOG_SHIFT_AGE'] = LOG_SHIFT_AGE
      ret['config.LOG_SHIFT_SIZE'] = LOG_SHIFT_SIZE
      ret['config.LOG_PATH'] = File.expand_path(LOG_PATH)
      ret['config.RTTABLE_PATH'] = File.expand_path(RTTABLE_PATH)
      ret['config.STORAGE_DELMARK_EXPTIME'] = STORAGE_DELMARK_EXPTIME
      ret['config.DATACOPY_STREAM_COPY_WAIT_PARAM'] = DATACOPY_STREAM_COPY_WAIT_PARAM
      ret['config.PLUGIN_FILES'] = PLUGIN_FILES.inspect
      ret['config.WRITEBEHIND_PATH'] = File.expand_path(WRITEBEHIND_PATH)
      ret['config.WRITEBEHIND_SHIFT_SIZE'] = WRITEBEHIND_SHIFT_SIZE
      ret
    end

  end # module Config

end # module Roma
