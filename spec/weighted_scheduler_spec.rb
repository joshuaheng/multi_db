require File.expand_path(File.dirname(__FILE__) + '/spec_helper')

describe MultiDb::WeightedScheduler do
  before do
    MultiDb::ConnectionProxy.setup! MultiDb::WeightedScheduler
    @scheduler = ActiveRecord::Base.connection_proxy.scheduler
    @sql = 'SELECT 1 + 1 FROM DUAL'
  end

  describe 'weight' do
    it 'should sum to the total weight of the slaves' do
      total_weight = @scheduler.items.map { |item| item::WEIGHT }.sum
      @scheduler.total_weight.should == total_weight
    end

    it 'should cache the total weight' do
      @scheduler.should_receive(:items).once.and_return([MultiDb::TestSlaveDatabase1])
      @scheduler.total_weight
      @scheduler.total_weight
    end
  end

  describe 'next_index!' do
    it 'should distribute the queries according to weight' do
      n = 100_000
      indices = n.times.map do
        @scheduler.send( :next_index! )
      end

      freqs = @scheduler.items.map { 0 }
      indices.each { |i| freqs[i] += 1 }

      freqs.each_with_index do |freq, i|
        expected = (@scheduler.items[i]::WEIGHT / @scheduler.total_weight.to_f).round
        actual   = (freq / n.to_f).round
        expected.should == actual
      end
    end
  end

end
