module StalkClimber
  class Climber
    include LazyEnumerable

    attr_accessor :beanstalk_addresses, :test_tube
    attr_reader :cache

    def connection_pool
      return @connection_pool unless @connection_pool.nil?
      if self.beanstalk_addresses.nil?
        raise RuntimeError, 'beanstalk_addresses must be set in order to establish a connection'
      end
      @connection_pool = ConnectionPool.new(self.beanstalk_addresses)
    end


    def climb(&block)
      threads = []
      self.connection_pool.connections.each do |connection|
        threads << Thread.new { connection.each(&block) }
      end
      threads.each(&:join)
      return
    end
    alias_method :each, :climb


    def initialize
      yield(self) if block_given?
    end

  end
end
