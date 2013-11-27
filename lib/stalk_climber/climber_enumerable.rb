module StalkClimber

  module ClimberEnumerable

    include RUBY_VERSION >= '2.0.0' ? LazyEnumerable : Enumerable
    include BreakableEnumerator

    attr_reader :climber


    # Factory that simplifies the creation of ClimberEnumerable classes.
    # Otherwise in simple cases the class would have to be defined only to
    # include this module and set the desired :enumerator_method
    def self.new(enumerator_method)
      return Class.new do
        include StalkClimber::ClimberEnumerable
        @enumerator_method = enumerator_method
      end
    end


    # Add :enumerator_method class level accessor to inheriting class
    def self.included(base)
      class << base
        attr_reader :enumerator_method
      end
    end


    # Iterate over all elements on all connections in the climber's connection pool.
    # An instance of the element is yielded. For more information see the method on Connection
    # designated :enumerator_method by the implementing class
    def each
      return to_enum unless block_given?
      return breakable_enumerator(to_enum, &Proc.new)
    end


    # Perform a threaded iteration across all connections in the climber's
    # connection pool. This method cannot be used for enumerable enumeration
    # because a break called within one of the threads will cause a LocalJumpError.
    # This could be fixed, but expected behavior on break varies as to whether
    # or not to wait for all threads before returning a result. However, still
    # useful for operations that always visit all elements.
    # An instance of the element is yielded with each iteration.
    def each_threaded(&block)
      threads = []
      climber.connection_pool.connections.each do |connection|
        threads << Thread.new { connection.send(self.class.enumerator_method, &block) }
      end
      threads.each(&:join)
      return
    end


    # Create a new instance of a ClimberEnumerable when given +climber+ that
    # references the StalkClimber that owns it
    def initialize(climber)
      @climber = climber
    end


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

  end

end
