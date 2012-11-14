require File.expand_path(File.dirname(__FILE__) + '/spec_helper')

describe MultiDb::ConnectionProxy do
  before do
    @sql = 'SELECT 1 + 1 FROM DUAL'
  end

  describe 'master_models' do
    before(:all) do
    end

    it 'should not get proxies when the classname is set' do
      MultiDb::ConnectionProxy.master_models = ['MasterModel']
      MultiDb::ConnectionProxy.setup!
      MasterModel.connection.should_not be_kind_of(MultiDb::ConnectionProxy)
    end

    it 'should not get proxies when the class is set' do
      MultiDb::ConnectionProxy.master_models = [MasterModel]
      MultiDb::ConnectionProxy.setup!
      MasterModel.connection.should_not be_kind_of(MultiDb::ConnectionProxy)
    end
  end

  describe 'setup!' do
    it 'ActiveRecord::Base should respond to #connection_proxy' do
      MultiDb::ConnectionProxy.setup!
      ActiveRecord::Base.connection_proxy.should be_kind_of(MultiDb::ConnectionProxy)
    end

    it 'TestModel#connection should return an instance of MultiDb::ConnectionProxy' do
      MultiDb::ConnectionProxy.setup!
      TestModel.connection.should be_kind_of(MultiDb::ConnectionProxy)
    end

    it 'should generate slave classes for database.yml entries' do
      slaves = {
        'test_slave_database_1'       => 'MultiDb::TestSlaveDatabase1',
        'test_slave_database_2'       => 'MultiDb::TestSlaveDatabase2',
        'test_slave_database_3'       => 'MultiDb::TestSlaveDatabase3',
        'test_slave_database_4'       => 'MultiDb::TestSlaveDatabase4',
        'test_extra_slave_database_1' => 'MultiDb::TestExtraSlaveDatabase1',
        'test_extra_slave_database_2' => 'MultiDb::TestExtraSlaveDatabase2'
      }

      MultiDb::ConnectionProxy.setup!

      slaves.each do |spec, slave|
        defined?(slave.constantize).should be_true
        slave_class = slave.constantize
        slave_class.proxy_spec.should == spec
        slave_class.connection_proxy.class.should == slave_class.retrieve_connection.class
      end
    end

    it 'should not generate slave classes for unused database.yml entries' do
      defined?(MultiDb::UnusedSlaveDatabase).should be_false
      MultiDb::ConnectionProxy.setup!

      defined?(MultiDb::TestSlaveDatabase1).should be_true
      defined?(MultiDb::TestSlaveDatabase2).should be_true
      defined?(MultiDb::TestSlaveDatabase3).should be_true
      defined?(MultiDb::TestSlaveDatabase4).should be_true

      defined?(MultiDb::TestExtraSlaveDatabase1).should be_true
      defined?(MultiDb::TestExtraSlaveDatabase2).should be_true
    end

    it 'should not generate slave classes for unused database.yml entries' do
      MultiDb::ConnectionProxy.setup!
      defined?(MultiDb::UnusedSlaveDatabase).should be_false
    end

    it 'should create a connection proxy with slaves for each established connection' do
      specs = {
        TestModel   => 'test',
        MasterModel => 'test',
        ExtraModel  => 'test_extra'
      }

      slaves = {
        TestModel   => [MultiDb::TestSlaveDatabase1, MultiDb::TestSlaveDatabase2, MultiDb::TestSlaveDatabase3,  MultiDb::TestSlaveDatabase4],
        MasterModel => [MultiDb::TestSlaveDatabase1, MultiDb::TestSlaveDatabase2, MultiDb::TestSlaveDatabase3,  MultiDb::TestSlaveDatabase4],
        ExtraModel  => [MultiDb::TestExtraSlaveDatabase1, MultiDb::TestExtraSlaveDatabase2]
      }

      MultiDb::ConnectionProxy.setup!

      specs.each do |model, spec|
        proxy = model.connection_proxy
        proxy.should be_a(MultiDb::ConnectionProxy)
        proxy.master.should == model
        proxy.scheduler.slaves.should == slaves[model]
      end
    end

    it 'should not create a proxy for models using connections with no slaves' do
      MultiDb::ConnectionProxy.setup!
      NoSlavesModel.connection_proxy.should_not be_a(MultiDb::ConnectionProxy)
    end

    describe 'weights' do
      before do
        MultiDb::ConnectionProxy.setup!
      end

      it 'should default slave weight to 1' do
        MultiDb::TestSlaveDatabase1::WEIGHT.should == 1
      end

      it 'should assign slave weights as configured in database.yml' do
        MultiDb::TestSlaveDatabase2::WEIGHT.should == 10
      end

      it 'should set slave weight to 1 if it is configured to be 0' do
        MultiDb::TestSlaveDatabase3::WEIGHT.should == 1
      end
    end

    describe 'defaults_to_master' do
      before do
        MultiDb::ConnectionProxy.defaults_to_master = true
        MultiDb::ConnectionProxy.setup!
        @proxy = TestModel.connection_proxy
      end

      after do
        MultiDb::ConnectionProxy.defaults_to_master = false
      end

      it 'should set the default database to master' do
        @proxy.current.should == @proxy.master
      end

      it 'should continue to use master when in a with_master block' do
        @proxy.with_master do
          @proxy.current.should == @proxy.master
        end
      end

      it 'should use slaves when in a with_slave block' do
        @proxy.with_slave do
          @proxy.current.should_not == @proxy.master
          @proxy.scheduler.slaves.should include(@proxy.current)
        end
      end

      it 'should handle nested with_slave and with_master blocks' do
        lambda { |nester, depth|
          @proxy.with_slave do
            @proxy.current.should_not == @proxy.master
            @proxy.scheduler.slaves.should include(@proxy.current)

            @proxy.with_master do
              @proxy.current.should == @proxy.master
              nester.call(nester, depth - 1) if depth > 0
            end
          end
        }.tap { |nester| nester.call nester, 10 }
      end
    end
  end

  describe 'db access' do
    before do
      MultiDb::ConnectionProxy.setup!
      @proxy = TestModel.connection_proxy

      @master_conn = @proxy.master.retrieve_connection
      @slave_conns = @proxy.scheduler.slaves.map &:retrieve_connection
    end

    def setup_slave_expectations(call, call_count, starting_index = 0)
      expectations = @slave_conns.map { 0 }

      call_count.times do |i|
        i = (i + starting_index) % @slave_conns.length
        expectations[i] = expectations[i] + 1
      end

      expectations.each_with_index do |expectation, i|
        @slave_conns[i].should_receive(call).exactly(expectation || 0)
      end
    end

    it 'should handle nested with_master blocks' do
      @proxy.current.should_not == @proxy.master

      lambda { |nester, depth|
        @proxy.with_master do
          @proxy.current.should == @proxy.master
          nester.call(nester, depth - 1) if depth > 0
        end
      }.tap { |nester| nester.call nester, 10 }

      @proxy.current.should_not == @proxy.master
    end

    it 'should perform transactions on the master' do
      @master_conn.should_receive(:select_all).exactly(:once)
      TestModel.transaction { @proxy.select_all @sql }

      #Should go to a slave
      @proxy.select_all @sql
    end

    it 'should switch to the next slave on selects' do
      requests = 6
      setup_slave_expectations :select_one, requests
      requests.times do
        @proxy.select_one @sql
      end
    end

    it 'should send dangerous methods to the master' do
      [:insert, :update, :delete, :execute].each do |method|
        @master_conn.should_receive method
        @proxy.send method, @sql
      end
    end

    it 'should dynamically generate safe methods' do
      # Can't really test all of them, just pick one that is unused in other tests
      method = :select_value
      @proxy.should_not respond_to(method)
      @proxy.send method, @sql
      @proxy.should respond_to(method)
    end

    it 'should not dynamically generate other methods' do
      other_methods = [:some, :weird, :things]
      other_methods.each do |method|
        @proxy.should_not respond_to(method)
        begin
          @proxy.send method
        rescue
        end
        @proxy.should_not respond_to(method)
      end
    end

    it 'should cache queries using select_all' do
      TestModel.cache do
        # next_reader will be called and move to the second slave
        @slave_conns[0].should_not_receive(:select_all)
        @master_conn.should_not_receive(:select_all)

        @slave_conns[1].should_receive(:select_all).exactly(:once)

        3.times { @proxy.select_all @sql }
      end
    end

    it 'should invalidate the cache on insert, delete, and update' do
      methods = [:insert, :update, :delete]
      requests = methods.length

      # This starts on the second slave
      setup_slave_expectations :select_all, requests, 1

      ActiveRecord::Base.cache do
        methods.each do |method|
          @master_conn.should_receive(method).and_return(true)

          @proxy.select_all @sql
          @proxy.send method
        end
      end
    end

    it 'should retry the next slave when one fails' do
      # This starts on the second slave
      @slave_conns[1].should_receive(:select_all).and_raise(RuntimeError)
      @slave_conns[2].should_receive(:select_all).and_return(true)
      @proxy.select_all @sql
    end

    it 'should fall back to the master if all slaves fail' do
      @slave_conns.each do |slave_conn|
        slave_conn.should_receive(:select_all).once.and_raise(RuntimeError)
      end
      @master_conn.should_receive(:select_all).and_return(true)
      @proxy.select_all @sql
    end

    it 'should try to reconnect to master after it has failed' do
      @master_conn.should_receive(:update).and_raise(RuntimeError)
      lambda { @proxy.update @sql }.should raise_error

      @master_conn.should_receive(:reconnect!).and_return(true)
      @master_conn.should_receive(:insert).and_return(true)
      @proxy.insert @sql
    end

    it 'should reload models from the master' do
      @slave_conns.each do |slave_conn|
        slave_conn.should_not_receive :select_all
      end

      persisted = 'baz'
      foo = TestModel.create!(:bar => persisted)
      foo.bar = 'unpersisted'

      foo.reload

      foo.bar.should == persisted
    end

    describe 'with sticky_slave enabled' do
      before do
        MultiDb::ConnectionProxy.sticky_slave = true
      end

      after do
        MultiDb::ConnectionProxy.sticky_slave = false
      end

      it 'should not switch to the next slave' do
        requests = 3
        @slave_conns[0].should_receive(:select_all).exactly(requests)
        @slave_conns[1..-1].each do |slave_conn|
          slave_conn.should_not_receive :select_all
        end

        requests.times { @proxy.select_all @sql }
      end

      it 'should still switch to next slave when next_reader! is called' do
        requests1 = 3
        requests2 = 7
        @slave_conns[0].should_receive(:select_one).exactly(requests1)
        @slave_conns[1].should_receive(:select_one).exactly(requests2)
        @slave_conns[2..-1].each do |slave_conn|
          slave_conn.should_not_receive :select_one
        end

        requests1.times { @proxy.select_one @sql }
        @proxy.next_reader!
        requests2.times { @proxy.select_one @sql }
      end
    end

    describe 'with a readable master' do
      before do
        @proxy = ReadableMasterModel.connection_proxy
      end

      it 'should read from master in addition to slaves' do
        @proxy.scheduler.slaves.each do |slave|
          slave.retrieve_connection.should_receive(:select_all)
        end
        @proxy.master.retrieve_connection.should_receive(:select_all)

        (@proxy.scheduler.slaves.length + 1).times { @proxy.select_all }
      end
    end

    describe 'in multiple threads' do
      it 'should keep #current and #next_reader! local to the thread' do
        @proxy.current.retrieve_connection.should      == @slave_conns[0]
        @proxy.next_reader!.retrieve_connection.should == @slave_conns[1]

        Thread.new do
          @proxy.current.retrieve_connection.should      == @slave_conns[0]
          @proxy.next_reader!.retrieve_connection.should == @slave_conns[1]
          @proxy.next_reader!.retrieve_connection.should == @slave_conns[2]
        end

        @proxy.current.retrieve_connection.should == @slave_conns[1]
      end

      it 'should keep with_master blocks local to the thread' do
        @proxy.current.should_not == @proxy.master

        @proxy.with_master do
          @proxy.current.should == @proxy.master

          Thread.new do
            @proxy.current.should_not == @proxy.master
            @proxy.with_master do
              @proxy.current.should == @proxy.master
            end
            @proxy.current.should_not == @proxy.master
          end

          @proxy.current.should == @proxy.master
        end

        @proxy.current.should_not == @proxy.master
      end

      it 'should switch to the next reader even whithin with_master-block in different threads' do
        # Because of connection pooling in AR, the second thread create a new connection behind the scenes.
        # Therefore, test that these connections are being retrieved for the correct databases.
        @proxy.master.should_not_receive(:retrieve_connection)

        MultiDb::TestSlaveDatabase1.should_receive(:retrieve_connection).twice.and_return(@slave_conns[0])
        MultiDb::TestSlaveDatabase2.should_receive(:retrieve_connection).once.and_return( @slave_conns[1])
        MultiDb::TestSlaveDatabase3.should_receive(:retrieve_connection).once.and_return( @slave_conns[2])
        MultiDb::TestSlaveDatabase4.should_receive(:retrieve_connection).once.and_return( @slave_conns[3])

        @proxy.with_master do
          Thread.new do
            5.times { @proxy.select_one(@sql) }
          end.join
        end
      end
    end

  end
end
