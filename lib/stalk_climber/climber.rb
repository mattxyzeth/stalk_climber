module StalkClimber
  class Climber
    include RUBY_VERSION >= '2.0.0' ? LazyEnumerable : Enumerable

    attr_accessor :beanstalk_addresses, :test_tube
    attr_reader :cache

    # Returns or creates a ConnectionPool from beanstalk_addresses
    def connection_pool
      return @connection_pool unless @connection_pool.nil?
      if self.beanstalk_addresses.nil?
        raise RuntimeError, 'beanstalk_addresses must be set in order to establish a connection'
      end
      @connection_pool = ConnectionPool.new(self.beanstalk_addresses, self.test_tube)
    end


    # Climb over all jobs on all connections in the connection pool.
    # An instance of Job is yielded to +block+
    def climb(&block)
      self.connection_pool.connections.each do |connection|
        connection.each(&block)
      end
    end
    alias_method :each, :climb


    # Perform a threaded climb across all connections in the connection pool.
    # This method cannot be used for enumerable enumeration because a break
    # called from one of the threads will cause a LocalJumpError
    # An instance of Job is yielded to +block+
    def climb_threaded(&block)
      threads = []
      self.connection_pool.connections.each do |connection|
        threads << Thread.new { connection.each(&block) }
      end
      threads.each(&:join)
      return
    end
    alias_method :each_threaded, :climb_threaded


    # Creates a new Climber instance, optionally yielding the instance
    # if a block is given
    def initialize(beanstalk_addresses = nil, test_tube = nil)
      self.beanstalk_addresses = beanstalk_addresses
      self.test_tube = test_tube
      yield(self) if block_given?
    end

  end
end
