require File.expand_path(File.dirname(__FILE__) + '/spec_helper')

describe MultiDb::Scheduler do

  before do
    @slaves = [5, 7, 4, 8]
    @scheduler = MultiDb::Scheduler.new(@slaves)
  end

  it 'should return slaves in a round robin fashion' do
    @scheduler.current.should == @slaves.first
    @slaves[1..-1].each do |slave|
      @scheduler.next.should == slave
    end
    @scheduler.next.should == @slaves.first
  end

  it 'should not return blacklisted slaves' do
    @scheduler.blacklist! @slaves[2]
    @slaves.length.times do
      @scheduler.next.should_not == @slaves[2]
    end
  end

  it 'should raise NoMoreslaves if all are blacklisted' do
    @slaves.each do |slave|
      @scheduler.blacklist!(slave)
    end
    lambda { @scheduler.next }.should raise_error(MultiDb::Scheduler::NoMoreSlaves)
  end

  it 'should unblacklist slaves automatically' do
    @scheduler = MultiDb::Scheduler.new(@slaves.clone, 1.second)
    @scheduler.blacklist! @slaves[1]
    sleep 1
    @scheduler.next.should == @slaves[1]
  end

  describe 'in multiple threads' do
    it 'should keep #current and #next local to the thread' do
      @scheduler.current.should == @slaves[0]
      @scheduler.next.should == @slaves[1]
      Thread.new do
        @scheduler.current.should == @slaves[0]
        @scheduler.next.should == @slaves[1]
      end.join
      @scheduler.next.should == @slaves[2]
    end

  end

end

