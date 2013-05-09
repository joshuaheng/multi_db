YAML::ENGINE.yamler = 'syck'

%w[active_record yaml erb rspec logger mysql2].each { |lib| require lib }

require File.expand_path('../../lib/multi_db', __FILE__)

module Rails
  def self.env
    ActiveSupport::StringInquirer.new("test")
  end
end

# Don't allow randomized slave access for the tests
MultiDb::Scheduler.initial_index = 0

ActiveRecord::Base.logger = Logger.new(File.expand_path('../debug.log', __FILE__))
ActiveRecord::Base.configurations = YAML::load(File.open(File.expand_path('../config/database.yml', __FILE__)))

ActiveRecord::Migration.verbose = false

ActiveRecord::Base.establish_connection :default
class NoSlavesModel < ActiveRecord::Base
  establish_connection :default
end

ActiveRecord::Base.establish_connection :test_extra
ActiveRecord::Migration.create_table(:extra_models, :force => true) {|t| t.string :pub}
class ExtraModel < ActiveRecord::Base
  establish_connection :test_extra
end

ActiveRecord::Base.establish_connection :readable_master
ActiveRecord::Migration.create_table(:readable_master_models, :force => true) {}
class ReadableMasterModel < ActiveRecord::Base
  establish_connection :readable_master
end

ActiveRecord::Base.establish_connection :test
ActiveRecord::Migration.create_table(:test_models, :force => true) {|t| t.string :bar}
class TestModel < ActiveRecord::Base; end

ActiveRecord::Migration.create_table(:master_models, :force => true) {}
class MasterModel < ActiveRecord::Base; end
