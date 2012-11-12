module MultiDb
  class WeightedScheduler < Scheduler

    def total_weight
      @total_weight ||= slaves.sum { |slave| slave::WEIGHT }
    end

    protected

    def next_index!
      i = rand(total_weight)
      slave = slaves.detect do |slave|
        i -= slave::WEIGHT
        true if i < 0
      end
      self.current_index = slaves.index slave
    end

  end
end

