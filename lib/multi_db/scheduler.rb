module MultiDb
  class Scheduler
    extend ThreadLocalAccessors

    class NoMoreSlaves < Exception; end

    def self.initial_index
      @initial_index
    end

    def self.initial_index=(index)
      @initial_index = index
    end

    attr_accessor :slaves
    delegate :[], :[]=, :to => :slaves
    tlattr_accessor :current_index, true

    def initialize(slaves, blacklist_timeout = 1.minute)
      @slaves = slaves
      @slave_count = slaves.length
      @blacklist = Array.new(@slave_count, Time.at(0))
      @blacklist_timeout = blacklist_timeout
      self.current_index = Scheduler.initial_index || rand(@slave_count)
    end

    def blacklist!(slave)
      @blacklist[@slaves.index(slave)] = Time.now
    end

    def current
      @slaves[current_index]
    end

    def next
      previous = current_index
      until(@blacklist[next_index!] < Time.now - @blacklist_timeout) do
        raise NoMoreSlaves, 'All slaves are blacklisted' if current_index == previous
      end
      current
    end

    protected

    def next_index!
      self.current_index = (current_index + 1) % @slave_count
    end

  end
end
