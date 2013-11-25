module StalkClimber
  class Climber
    include RUBY_VERSION >= '2.0.0' ? LazyEnumerable : Enumerable

    attr_accessor :beanstalk_addresses, :test_tube
    attr_reader :jobs

    # Returns or creates a ConnectionPool from beanstalk_addresses
    def connection_pool
      return @connection_pool unless @connection_pool.nil?
      if self.beanstalk_addresses.nil?
        raise RuntimeError, 'beanstalk_addresses must be set in order to establish a connection'
      end
      @connection_pool = ConnectionPool.new(self.beanstalk_addresses, self.test_tube)
    end


    # Creates a new Climber instance, optionally yielding the instance
    # if a block is given
    def initialize(beanstalk_addresses = nil, test_tube = nil)
      self.beanstalk_addresses = beanstalk_addresses
      self.test_tube = test_tube
      @jobs = StalkClimber::Jobs.new(self)
      yield(self) if block_given?
    end


    # Returns a hash with connections as keys and max_job_ids as values
    def max_job_ids
      connection_pairs = connection_pool.connections.map do |connection|
        [connection, connection.max_job_id]
      end
      return Hash[connection_pairs]
    end

  end
end
