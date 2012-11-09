module MultiDb
  module ActiveRecordRuntimeExtensions
    def self.included(base)
      base.send :include, InstanceMethods
      base.send :extend, ClassMethods
      base.cattr_accessor :connection_proxy
      # handle subclasses which were defined by the framework or plugins
      base.send(:descendants).each do |child|
        child.hijack_connection
      end
    end

    module InstanceMethods
      def reload(options = nil)
        self.connection_proxy.with_master { super }
      end
    end

    module ClassMethods
      # Make sure transactions always switch to the master
      def transaction(options = {}, &block)
        if self.connection.kind_of?(ConnectionProxy)
          super
        else
          self.connection_proxy.with_master { super }
        end
      end

      # make caching always use the ConnectionProxy
      def cache(&block)
        if ActiveRecord::Base.configurations.blank?
          yield
        else
          self.connection_proxy.cache(&block)
        end
      end

      def inherited(child)
        super
        child.hijack_connection
      end

      def hijack_connection
        return if ConnectionProxy.master_models.include?(self.to_s)
        logger.info "[MULTIDB] hijacking connection for #{self.to_s}"
        class << self
          def connection
            self.connection_proxy
          end
        end
      end
    end
  end

  #TODO: This isn't working at all
  module ActiveRecordExtensions
    def self.included(base)
      base.cattr_accessor :proxy_spec
    end

    def self.establish_connection(spec = nil)
      self.proxy_spec = spec.to_s if spec.is_a?(String) || spec.is_a?(Symbol)
      super
    end
  end
end

ActiveRecord::Base.send :include, MultiDb::ActiveRecordExtensions
