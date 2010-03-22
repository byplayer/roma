module Roma
  module CommandPlugin
    @@plugins=[]
    def self.plugins
      @@plugins.dup
    end

    def self.included(mod)
      @@plugins << mod
    end

    def self.plugins_shift
      @@plugins.shift
    end

    def self.plugins_size
      @@plugins.size
    end
  end
end
