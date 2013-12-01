module StalkClimber
  class Tubes < Beaneater::Tubes

    extend Forwardable
    include RUBY_VERSION >= '2.0.0' ? LazyEnumerable : Enumerable

    def_delegator :all, :each


    # :call-seq:
    #   all() => Array[StalkClimber::Tube]
    #
    # List of all known beanstalk tubes in the connection pool.
    # Adapted with minor modification from Beaneater::Tubes#all
    def all
      return transmit_to_all('list-tubes', :merge => true)[:body].map do |tube_name|
        StalkClimber::Tube.new(self.pool, tube_name)
      end
    end

    # :call-seq:
    #   each() => Enumerator
    #   each {|tube| block }
    #
    # Interface for tube enumerator/enumeration. Tubes are mostly in creation order,
    # though order can vary depending on the order in which responses to list-tubes are
    # received from each of the Beanstalkd servers. Returns an instance of
    # StalkClimber::Tube for each tube. Unlike Jobs, tubes are not cached because
    # they are more easily accessible
    #
    #   tubes = StalkClimber::Tubes.new(connection_pool)
    #   tubes.each do |tube|
    #     tube.clear
    #   end

  end
end
