module StalkClimber
  class Climber
    include LazyEnumerable

    attr_accessor :beanstalk_addresses, :test_tube
    attr_reader :cache

    # Returns or creates a ConnectionPool from beanstalk_addresses
    def connection_pool
      return @connection_pool unless @connection_pool.nil?
      if self.beanstalk_addresses.nil?
        raise RuntimeError, 'beanstalk_addresses must be set in order to establish a connection'
      end
      @connection_pool = ConnectionPool.new(self.beanstalk_addresses)
    end


    # Perform a threaded climb across all connections in the connection pool.
    # An instance of Job is yielded to +block+
    def climb(&block)
      threads = []
      self.connection_pool.connections.each do |connection|
        threads << Thread.new { connection.each(&block) }
      end
      threads.each(&:join)
      return
    end
    alias_method :each, :climb


    # Creates a new Climber instance, optionally yielding the instance
    # if a block is given
    def initialize
      yield(self) if block_given?
    end

  end
end
