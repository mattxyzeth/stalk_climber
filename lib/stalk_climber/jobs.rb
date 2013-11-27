module StalkClimber

  class Jobs

    include RUBY_VERSION >= '2.0.0' ? LazyEnumerable : Enumerable
    include BreakableEnumerator

    attr_reader :climber


    # Iterate over all jobs on all connections in the climber's connection pool.
    # An instance of Job is yielded. For more information see Connection#each_job
    def each
      return to_enum unless block_given?
      return breakable_enumerator(to_enum, &Proc.new)
    end


    # Perform a threaded iteration across all connections in the climber's
    # connection pool. This method cannot be used for enumerable enumeration
    # because a break called from one of the threads will cause a LocalJumpError.
    # This could be fixed, but expected behavior on break varies as to whether
    # or not to wait for all threads before returning a result. However, still
    # useful for operations that always visit all jobs.
    # An instance of Job is yielded to +block+
    def each_threaded(&block)
      threads = []
      climber.connection_pool.connections.each do |connection|
        threads << Thread.new { connection.each_job(&block) }
      end
      threads.each(&:join)
      return
    end


    # Create a new instance of StalkClimber::Jobs when given +climber+ that
    # references the StalkClimber that owns it
    def initialize(climber)
      @climber = climber
    end


    # Returns an Enumerator for enumerating jobs on all connections.
    # Connections are enumerated in the order defined. See Connection#to_enum
    # for more information
    # A job is yielded with each iteration.
    def to_enum
      return Enumerator.new do |yielder|
        climber.connection_pool.connections.each do |connection|
          connection.each_job do |job|
            yielder << job
          end
        end
      end
    end

  end

end
