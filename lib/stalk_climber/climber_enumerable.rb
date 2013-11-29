module StalkClimber

  module ClimberEnumerable

    include RUBY_VERSION >= '2.0.0' ? LazyEnumerable : Enumerable
    extend Forwardable

    def_delegators :to_enum, :each

    # A reference to the climber instance to which this enumerable belongs
    attr_reader :climber


    # :call-seq:
    #   new(enumerator_method) => New class including ClimberEnumerable
    #
    # Factory that simplifies the creation of ClimberEnumerable classes.
    # Otherwise in simple cases a class would have to be defined only to
    # include this module and set the desired :enumerator_method symbol.
    # The :enumerator_method parameter is passed to each connection in
    # the connection pool of the climber given at instantiation.
    #
    #   jobs = ClimberEnumerable.new(:each_job)
    #   instance = jobs.new(climber)
    #   instance.each do |job|
    #     break job
    #   end
    #     #=> #<StalkClimber::Job id=1 body="Work to be done">
    #
    def self.new(enumerator_method)
      return Class.new do
        include StalkClimber::ClimberEnumerable
        @enumerator_method = enumerator_method

        # Create a new instance of a ClimberEnumerable when given +climber+ that
        # references the StalkClimber that owns it
        def initialize(climber)
          @climber = climber
        end
      end
    end


    # Add :enumerator_method class level accessor to inheriting class
    def self.included(base) # :nodoc:
      class << base
        attr_reader :enumerator_method
      end
    end


    # Perform a threaded iteration across all connections in the climber's
    # connection pool. This method cannot be used for enumerable enumeration
    # because a break called within one of the threads will cause a LocalJumpError.
    # This could be fixed, but expected behavior on break varies as to whether
    # or not to wait for all threads before returning a result. However, still
    # useful for operations that always visit all elements.
    # An instance of the element is yielded with each iteration.
    #
    #   jobs = ClimberEnumerable.new(:each_job)
    #   instance = jobs.new(climber)
    #   instance.each_threaded do |job|
    #     ...
    #   end
    def each_threaded(&block) # :yields: Object
      threads = []
      climber.connection_pool.connections.each do |connection|
        threads << Thread.new { connection.send(self.class.enumerator_method, &block) }
      end
      threads.each(&:join)
      return
    end


    # :call-seq:
    #   to_enum() => Enumerator
    #
    # Returns an Enumerator for enumerating elements on all connections.
    # Connections are enumerated in the order defined. See Connection#to_enum
    # for more information
    # An instance of the element is yielded with each iteration.
    def to_enum
      return Enumerator.new do |yielder|
        climber.connection_pool.connections.each do |connection|
          connection.send(self.class.enumerator_method) do |element|
            yielder << element
          end
        end
      end
    end


    # :call-seq:
    #   each {|obj| block} => Object
    #
    # Iterate over all elements on all connections in the climber's connection
    # pool. If no block is given, returns the enumerator provided by #to_enum.
    # An instance of the element is yielded to a given block. For more information
    # see the method on Connection designated :enumerator_method by the implementing class
    #
    #   jobs = ClimberEnumerable.new(:each_job)
    #   instance = jobs.new(climber)
    #   instance.each do |job|
    #     break job
    #   end
    #     #=> #<StalkClimber::Job id=1 body="Work to be done">

  end

end
