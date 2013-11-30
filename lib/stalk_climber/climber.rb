module StalkClimber
  class Climber

    extend Forwardable

    def_delegator :connection_pool, :tubes

    # Collection of beanstalk_addresses the pool is connected to
    attr_accessor :beanstalk_addresses

    # Accessor to the climber's Jobs instance
    attr_reader :jobs

    # Tube used when injecting jobs to probe state of Beanstalkd
    attr_accessor :test_tube

    # :call-seq:
    #   connection_pool() => StalkClimber::ConnectionPool
    #
    # Returns or creates a ConnectionPool from beanstalk_addresses. Raises a
    # RuntimeError if #beanstalk_addresses has not been set.
    def connection_pool
      return @connection_pool unless @connection_pool.nil?
      if self.beanstalk_addresses.nil?
        raise RuntimeError, 'beanstalk_addresses must be set in order to establish a connection'
      end
      @connection_pool = ConnectionPool.new(self.beanstalk_addresses, self.test_tube)
    end


    # Creates a new Climber instance, optionally yielding the instance for
    # configuration if a block is given
    #
    #   Climber.new('beanstalk://localhost:11300', 'stalk_climber')
    #     #=> #<StalkClimber::Job beanstalk_addresses="beanstalk://localhost:11300" test_tube="stalk_climber">
    def initialize(beanstalk_addresses = nil, test_tube = nil) # :yields: climber
      self.beanstalk_addresses = beanstalk_addresses
      self.test_tube = test_tube
      @jobs = StalkClimber::Jobs.new(self)
      yield(self) if block_given?
    end


    # :call-seq:
    #   max_job_ids() => Hash{Beaneater::Connection => Integer}
    #
    # Returns a Hash with connections as keys and max_job_ids as values
    #
    #   climber = Climber.new('beanstalk://localhost:11300', 'stalk_climber')
    #   climber.max_job_ids
    #     #=> {#<Beaneater::Connection host="localhost" port=11300>=>1183}
    def max_job_ids
      connection_pairs = connection_pool.connections.map do |connection|
        [connection, connection.max_job_id]
      end
      return Hash[connection_pairs]
    end


    # :call-seq:
    #   to_s() => String
    #
    # Return string representation of climber
    #
    #   Climber.new('beanstalk://localhost:11300', 'stalk_climber').to_s
    #     #=> #<StalkClimber::Job beanstalk_addresses="beanstalk://localhost:11300" test_tube="stalk_climber">
    def to_s
      return "#<StalkClimber::Job beanstalk_addresses=#{beanstalk_addresses.inspect} test_tube=#{test_tube.inspect}>"
    end
    alias :inspect :to_s

  end
end
