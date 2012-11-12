require File.expand_path(File.dirname(__FILE__) + '/spec_helper')

describe MultiDb::Scheduler do

  before do
    @items = [5, 7, 4, 8]
    @scheduler = MultiDb::Scheduler.new(@items)
  end

  it 'should return items in a round robin fashion' do
    @scheduler.current.should == @items.first
    @items[1..-1].each do |item|
      @scheduler.next.should == item
    end
    @scheduler.next.should == @items.first
  end

  it 'should not return blacklisted items' do
    @scheduler.blacklist! @items[2]
    @items.length.times do
      @scheduler.next.should_not == @items[2]
    end
  end

  it 'should raise NoMoreItems if all are blacklisted' do
    @items.each do |item|
      @scheduler.blacklist!(item)
    end
    lambda { @scheduler.next }.should raise_error(MultiDb::Scheduler::NoMoreItems)
  end

  it 'should unblacklist items automatically' do
    @scheduler = MultiDb::Scheduler.new(@items.clone, 1.second)
    @scheduler.blacklist! @items[1]
    sleep 1
    @scheduler.next.should == @items[1]
  end

  describe 'in multiple threads' do
    it 'should keep #current and #next local to the thread' do
      @scheduler.current.should == @items[0]
      @scheduler.next.should == @items[1]
      Thread.new do
        @scheduler.current.should == @items[0]
        @scheduler.next.should == @items[1]
      end.join
      @scheduler.next.should == @items[2]
    end

  end

end

