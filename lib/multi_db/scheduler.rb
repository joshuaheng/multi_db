module MultiDb
  class Scheduler
    class NoMoreSlaves < Exception; end
    extend ThreadLocalAccessors

    def self.initial_index
      @initial_index
    end

    def self.initial_index=(index)
      @initial_index = index
    end

    attr :slaves
    delegate :[], :[]=, :to => :slaves
    tlattr_accessor :current_index, true

    def initialize(slaves, blacklist_timeout = 1.minute)
      @n = slaves.length
      @slaves = slaves
      @blacklist = Array.new(@n, Time.at(0))
      @blacklist_timeout = blacklist_timeout
      self.current_index = Scheduler.initial_index || rand(@n)
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
      self.current_index = (current_index + 1) % @n
    end

  end
end
