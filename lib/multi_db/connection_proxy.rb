require 'active_record/connection_adapters/abstract/query_cache'

module MultiDb
  class ConnectionProxy
    include ActiveRecord::ConnectionAdapters::QueryCache
    include QueryCacheCompatibility
    extend ThreadLocalAccessors

    # Safe methods will go only to the slave or to the current active connection
    SAFE_METHODS = [ :select_all, :select_one, :select_value, :select_values, 
      :select_rows, :select, :verify!, :raw_connection, :active?, :reconnect!,
      :disconnect!, :reset_runtime, :log, :log_info ]

    if ActiveRecord.const_defined?(:SessionStore) # >= Rails 2.3
      DEFAULT_MASTER_MODELS = ['ActiveRecord::SessionStore::Session']
    else # =< Rails 2.3
      DEFAULT_MASTER_MODELS = ['CGI::Session::ActiveRecordStore::Session']
    end

    attr_accessor :master
    tlattr_accessor :master_depth, :current, true

    class << self

      # Defaults to Rails.env if multi_db is used with Rails
      # Defaults to 'development' when used outside Rails
      attr_accessor :environment

      # A list of models that should always go directly to the master
      #
      # Example:
      #
      #   MultiDb::ConnectionProxy.master_models = ['MySessionStore', 'PaymentTransaction']
      def master_models
        @master_models || DEFAULT_MASTER_MODELS
      end

      def master_models=(models)
        @master_models = models || []
      end

      # Decides if we should switch to the next reader automatically.
      # If true, a before or after filter should do this.
      # This will not affect failover if a master is unavailable.
      attr_accessor :sticky_slave

      # If master should be the default DB
      attr_accessor :defaults_to_master

      # Replaces the connection of your models with a proxy and
      # establishes the connections to the slaves
      def setup!(scheduler = Scheduler)
        self.environment   ||= (defined?(Rails) ? Rails.env : 'development')
        self.sticky_slave  ||= false

        ActiveRecord::Base.send :include, MultiDb::ActiveRecordRuntimeExtensions

        masters = ActiveRecord::Base.descendants << ActiveRecord::Base
        masters.each do |master|
          spec   = master.proxy_spec || self.environment
          slaves = init_slaves spec, master
          count  = "#{slaves.length} slave#{'s' unless slaves.length == 1}"

          if slaves.empty?
            slaves = [master]
            count = 'No slaves'
          end

          master.connection_proxy = new master, slaves, scheduler
          master.logger.info "[MULTIDB] #{count} loaded for #{master} from #{spec}"
        end
      end

      protected

      # Slave entries in the database.yml must be named like this
      #   development_slave_database_0:
      #   development_slave_database_1:
      #   ...
      # These would be available later as MultiDb::DevelopmentSlaveDatabase0, etc.
      def init_slaves(spec, master)
        slaves = ActiveRecord::Base.configurations.map do |name, values|
          if name.to_s =~ /^(#{spec}_slave_database.*)/
            weight = (values['weight'] || 1).to_i.abs
            weight = 1 if weight == 0

            slave_name  = $1.camelize
            slave_class = %Q{
              class #{slave_name} < ActiveRecord::Base
                self.abstract_class = true
                establish_connection :#{name}
                WEIGHT = #{weight} unless const_defined?('WEIGHT')
              end
            }

            MultiDb.module_eval slave_class, __FILE__, __LINE__
            "MultiDb::#{slave_name}"
          end
        end

        # Sorting obviously isn't necessary, but it makes testing a bit easier
        slaves.compact!
        slaves.sort!.map! &:constantize

        master_config = ActiveRecord::Base.configurations[spec]
        slaves << master if master_config && master_config['readable']

        slaves
      end

      private :new

    end

    def initialize(master, slaves, scheduler = Scheduler)
      @scheduler = scheduler.new(slaves)
      @master    = master
      @reconnect = false
      @query_cache = {}
      if self.class.defaults_to_master
        self.current = @master
        self.master_depth = 1
      else
        self.current = @scheduler.current
        self.master_depth = 0
      end
    end

    def slave
      @scheduler.current
    end

    def scheduler
      @scheduler
    end

    def with_master
      self.current = @master
      self.master_depth += 1
      yield
    ensure
      self.master_depth -= 1
      self.current = slave if (master_depth <= 0) 
    end

    def with_slave
      self.current = slave
      self.master_depth -= 1
      yield
    ensure
      self.master_depth += 1
      self.current = @master if (master_depth > 0)
    end

    def transaction(start_db_transaction = true, &block)
      with_master { @master.retrieve_connection.transaction(start_db_transaction, &block) }
    end

    # Calls the method on master/slave and dynamically creates a new
    # method on success to speed up subsequent calls
    def method_missing(method, *args, &block)
      send(target_method(method), method, *args, &block).tap do 
        create_delegation_method!(method)
      end
    end

    # Switches to the next slave database for read operations.
    # Fails over to the master database if all slaves are unavailable.
    def next_reader!
      # Don't if in with_master block
      return if  master_depth > 0

      self.current = @scheduler.next
    rescue Scheduler::NoMoreSlaves
      logger.warn "[MULTIDB] All slaves are blacklisted. Reading from master"
      self.current = @master
    end

    protected

    def create_delegation_method!(method)
      method_def = %Q{
        def #{method}(*args, &block)
          #{'next_reader!' unless self.class.sticky_slave || unsafe?(method)}
          #{target_method(method)}(:#{method}, *args, &block)
        end
      }
      self.instance_eval method_def, __FILE__, __LINE__
    end

    def target_method(method)
      unsafe?(method) ? :send_to_master : :send_to_current
    end

    def send_to_master(method, *args, &block)
      reconnect_master! if @reconnect
      @master.retrieve_connection.send(method, *args, &block)
    rescue => e
      raise_master_error(e)
    end

    def send_to_current(method, *args, &block)
      reconnect_master! if @reconnect && master?
      current.retrieve_connection.send(method, *args, &block)

    rescue NotImplementedError, NoMethodError
      raise

    # TODO Don't rescue everything
    rescue => e
      raise_master_error(e) if master?
      logger.warn "[MULTIDB] Error reading from slave database"
      logger.error %(#{e.message}\n#{e.backtrace.join("\n")})
      @scheduler.blacklist!(current)
      next_reader!
      retry
    end

    def reconnect_master!
      @master.retrieve_connection.reconnect!
      @reconnect = false
    end

    def raise_master_error(error)
      logger.fatal "[MULTIDB] Error accessing master database. Scheduling reconnect"
      @reconnect = true
      raise error
    end

    def unsafe?(method)
      !SAFE_METHODS.include?(method)
    end

    def master?
      current == @master
    end

    def logger
      ActiveRecord::Base.logger
    end

  end
end
