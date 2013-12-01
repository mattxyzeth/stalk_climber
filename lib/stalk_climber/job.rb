module StalkClimber
  class Job < Beaneater::Job

    # Attributes that are retrieved each request by default
    FRESH_ATTRIBUTES = %w[delay pri state time-left]

    # Attributes that return cached values by default
    CACHED_ATTRIBUTES = %w[age buries kicks releases reserves timeouts ttr tube]

    # All attributes available via a job's stats
    STATS_ATTRIBUTES = (CACHED_ATTRIBUTES + FRESH_ATTRIBUTES).sort!

    # Lookup table of which method to call to retrieve each stat
    STATS_METHODS = Hash[
      STATS_ATTRIBUTES.zip(STATS_ATTRIBUTES.map {|stat| stat.gsub(/-/, '_')})
    ]

    STATS_METHODS.each_pair do |stat, method_name|
      force = FRESH_ATTRIBUTES.include?(stat)
      define_method method_name do |force_refresh = force|
        return stats(force_refresh).send(method_name)
      end
    end


    # :call-seq:
    #   body() => String
    #
    # Returns or fetches the body of the job. If not already present the body of
    # the job is obtained via the peek command.
    #
    #   job = StalkClimber::Job.new(connection.transmit('reserve'))
    #   job.body
    #     #=> "Work to be done"
    def body
      return @body ||= connection.transmit("peek #{id}")[:body]
    end


    # :call-seq:
    #   delete() => Hash{Symbol => String,Integer}
    #
    # Deletes the job from beanstalk. Returns the Beanstalkd response for the command.
    #
    #   job = StalkClimber::Job.new(connection.transmit('reserve'))
    #   job.delete
    #     #=> {:status=>"DELETED", :body=>nil, :connection=>#<Beaneater::Connection host="localhost" port=11300>}
    def delete
      result = super
      @status = 'DELETED'
      @stats = nil
      @body = nil
      return result
    end


    # :call-seq:
    #   exists?() => Boolean
    #
    # Determines if a job exists by retrieving stats for the job. If Beaneater can't find
    # the job then it does not exist and false is returned. The stats command is used
    # because it will return a response of a near constant size, whereas, depending on
    # the job, the peek command could return a much larger response. Rather than waste
    # the trip to the server, stats are updated each time the method is called.
    #
    #   job = StalkClimber::Job.new(connection.transmit('reserve'))
    #   job.exists?
    #     #=> true
    #   job.delete
    #   job.exists?
    #     #=> false
    def exists?
      return @status == 'DELETED' ? false : super
    end


    # :call-seq:
    #   new(job_data) => Job
    #
    # Initializes a Job instance using +job_data+ which should be the Beaneater response to either
    # a put, peek, or stats-job command. Other Beaneater responses are not supported.
    #
    # No single beanstalk command provides all the data an instance might need, so as more
    # information is required, additional calls are made to beanstalk. For example, accessing both
    # a job's tube and its body requires both a peek and stats-job call.
    #
    # Put provides only the ID of the job and as such yields the least informed instance. Both a
    # peek and stats-job call may be required to retrieve anything but the ID of the instance
    #
    # Peek and reserve provide the ID and body of the job. A stats-job call may be required to
    # access anything but the ID or body of the job.
    #
    # Stats-job provides the most information about the job, but lacks the crtical component of the
    # job body. As such, a peek call would be required to access the body of the job.
    #
    #   reserve_response = connection.transmit('reserve')
    #   StalkClimber::Job.new(reserve_response)
    #     #=> #<StalkClimber::Job id=1 body="Work to be done">
    def initialize(job_data)
      case job_data[:status]
      when 'INSERTED' # put
        @id = job_data[:id].to_i
        @body = @stats = nil
      when 'FOUND', 'RESERVED' # peek, reserve
        @id = job_data[:id].to_i
        @body = job_data[:body]
        @stats = nil
      when 'OK' # stats-job
        @stats = Beaneater::StatStruct.from_hash(job_data[:body])
        @id = @stats.id.to_i
        @body = nil
      else
        raise RuntimeError, "Unexpected job status: #{job_data[:status]}"
      end
      @status = job_data[:status]
      @connection = job_data[:connection]
      # Don't set @reserved to force lookup each time to handle job TTR expiration
    end


    # :call-seq:
    #   stats(force_refresh = true) => Beaneater::StatStruct
    #
    # Returns or retrieves stats for the job. Optionally, a retrieve may be forced
    # by passing a non-false value for +force_refresh+
    #
    #   job = StalkClimber::Job.new(peek_response)
    #   job.stats
    #     #=> #<Beaneater::StatStruct id=2523, tube="stalk_climber", state="ready", pri=0, age=25, delay=0, ttr=120, time_left=0, file=0, reserves=0, timeouts=0, releases=0, buries=0, kicks=0>
    def stats(force_refresh = true)
      if @stats.nil? || force_refresh
        @stats = super()
      end
      return @stats
    end


    # :call-seq:
    #   to_h() => Hash
    #
    # Returns a hash of all job attributes derived from updated stats
    #
    #   job = StalkClimber::Job.new(peek_response)
    #   job.to_h
    #     #=> {"age"=>144, "body"=>"Work to be done", "buries"=>0, "connection"=>#<Beaneater::Connection host="localhost" port=11300>, "delay"=>0, "id"=>2523, "kicks"=>0, "pri"=>0, "releases"=>0, "reserves"=>0, "state"=>"ready", "time-left"=>0, "timeouts"=>0, "ttr"=>120, "tube"=>"stalk_climber"}
    def to_h
      stats
      stats_pairs = STATS_METHODS.map { |stat, method_name| [stat, stats(false)[method_name]]}
      stats_pairs.concat([['body', body], ['connection', connection], ['id', id]]).sort_by!(&:first)
      return Hash[stats_pairs]
    end


    # :call-seq:
    #   to_s() => String
    #
    # Returns string representation of job
    #
    #   job = StalkClimber::Job.new(peek_response)
    #   job.to_s
    #     #=> #<StalkClimber::Job id=1 body="Work to be done">
    def to_s
      "#<StalkClimber::Job id=#{id} body=#{body.inspect}>"
    end
    alias :inspect :to_s


    # :method: age
    # :call-seq:
    #   age(force_refresh = false) => Integer
    #
    # Retrieves the age of the job from the job's stats. Passing a non-false value for
    # +force_refresh+ will force retrieval of updated stats for the job
    #
    #   job = StalkClimber::Job.new(peek_response)
    #   job.age
    #     #=> 1024

    # :method: buries
    # :call-seq:
    #   buries(force_refresh = false) => Integer
    #
    # Retrieves the number of times the job has been buried from the job's stats. Passing
    # a non-false value for +force_refresh+ will force retrieval of updated stats for the job
    #
    #   job = StalkClimber::Job.new(connection.transmit('peek-buried'))
    #   job.buries
    #     #=> 1

    # :method: connection
    # :call-seq:
    #   connection() => Beaneater::Connection
    #
    # Returns the Beaneater::Connection instance which retrieved job.
    #
    #   job = StalkClimber::Job.new(peek_response)
    #   job.connection
    #     #=> #<Beaneater::Connection host="localhost" port=11300>

    # :method: id
    # :call-seq:
    #   id() => Integer
    #
    # Returns the id of the job.
    #
    #   job = StalkClimber::Job.new(peek_response)
    #   job.id
    #     #=> 1

    # :method: bury
    # :call-seq:
    #   bury() => Hash{Symbol => String,Integer}
    #
    # Sends command to bury a reserved job. Returns the Beanstalkd response for
    # the command.
    #
    #   job = StalkClimber::Job.new(connection.transmit('reserve'))
    #   job.bury
    #     #=> {:status=>"BURIED", :body=>nil, :connection=>#<Beaneater::Connection host="localhost" port=11300>}

    # :method: delay
    # :call-seq:
    #   delay(force_refresh = false) => Integer
    #
    # Retrieves the the remaining delay before the job is ready from the job's stats. Passing
    # a non-false value for +force_refresh+ will force retrieval of updated stats for the job.
    # By default the value for delay is refreshed every time it is requested.
    #
    #   job = StalkClimber::Job.new(connection.transmit('peek-delayed'))
    #   job.delay
    #     #=> 1024

    # :method: kick
    # :call-seq:
    #   kick() => Hash{Symbol => String,Integer}
    #
    # Sends command to kick a buried job. Returns the Beanstalkd response for the command.
    #
    #   job = StalkClimber::Job.new(connection.transmit('peek-buried'))
    #   job.kick
    #     #=> {:status=>"KICKED", :body=>nil, :connection=>#<Beaneater::Connection host="localhost" port=11300>}

    # :method: kicks
    # :call-seq:
    #   kicks(force_refresh = false) => Integer
    #
    # Retrieves the number of times the job has been kicked from the job's stats. Passing
    # a non-false value for +force_refresh+ will force retrieval of updated stats for the job
    # By default the value for kicks is cached and will not be updated unless forced.
    #
    #   job = StalkClimber::Job.new(connection.transmit('peek-buried'))
    #   job.kicks
    #     #=> 5

    # :method: pri
    # :call-seq:
    #   pri(force_refresh = false) => Integer
    #
    # Retrieves the priority of the job from the job's stats. Passing a non-false value for
    # +force_refresh+ will force retrieval of updated stats for the job.
    # By default the the value for pri is refreshed every time it is requested.
    #
    #   job = StalkClimber::Job.new(connection.transmit('peek-ready'))
    #   job.pri
    #     #=> 0

    # :method: release
    # :call-seq:
    #   release(options = {}) => Hash{Symbol => String,Integer}
    #
    # Sends command to release a job back to ready state. Return value
    # is the Beanstalkd response to the command.
    #
    # Options may include an Integer pri to set a new pri for the job and/or an
    # Integer delay to assign a new delay to the job
    #
    #   job = StalkClimber::Job.new(connection.transmit('reserve'))
    #   job.release(:delay => 120, :pri => 1024)
    #     #=> {:status=>"RELEASED", :body=>nil, :connection=>#<Beaneater::Connection host="localhost" port=11300>}

    # :method: releases
    # :call-seq:
    #   releases(force_refresh = false) => Integer
    #
    # Retrieves the number of times the job has been released from the job's stats. Passing
    # a non-false value for +force_refresh+ will force retrieval of updated stats for the job
    # By default the value for releases is cached and will not be updated unless forced.
    #
    #   job = StalkClimber::Job.new(connection.transmit('peek-ready'))
    #   job.releases
    #     #=> 3

    # :method: reserved?
    # :call-seq:
    #   reserved?() => Boolean
    #
    # Returns true if a job is currently in a reserved state.
    #
    #   job = StalkClimber::Job.new(connection.transmit('reserve'))
    #   job.reserved?
    #     #=> true

    # :method: reserves
    # :call-seq:
    #   reserves(force_refresh = false) => Integer
    #
    # Retrieves the number of times the job has been reserved from the job's stats. Passing
    # a non-false value for +force_refresh+ will force retrieval of updated stats for the job
    # By default the value for reserves is cached and will not be updated unless forced.
    #
    #   job = StalkClimber::Job.new(connection.transmit('reserve'))
    #   job.reserves
    #     #=> 1

    # :method: state
    # :call-seq:
    #   state(force_refresh = false) => String
    #
    # Retrieves the state of the job from the job's stats. Value will be one of "ready",
    # "delayed", "reserved", or "buried". Passing a non-false value for +force_refresh+
    # will force retrieval of updated stats for the job
    # By default the the value for state is refreshed every time it is requested.
    #
    #   job = StalkClimber::Job.new(connection.transmit('peek-ready'))
    #   job.state
    #     #=> 'ready'

    # :method: time_left
    # :call-seq:
    #   time_left(force_refresh = false) => Integer
    #
    # Retrieves the number of seconds left until the server puts this job into the ready
    # queue. This number is only meaningful if the job is reserved or delayed. If the job
    # is reserved and this amount of time elapses before its state changes, it is considered
    # to have timed out. Passing a non-false value for +force_refresh+ will force retrieval
    # of updated stats for the job By default the the value for time_left is refreshed every
    # time it is requested.
    #
    #   job = StalkClimber::Job.new(connection.transmit('reserve'))
    #   job.time_left
    #     #=> 119

    # :method: timeouts
    # :call-seq:
    #   timeouts(force_refresh = false) => Integer
    #
    # Retrieves the number of times the job has timed out from the job's stats. Passing
    # a non-false value for +force_refresh+ will force retrieval of updated stats for the job
    # By default the value for timeouts is cached and will not be updated unless forced.
    #
    #   job = StalkClimber::Job.new(connection.transmit('reserve'))
    #   job.timeouts
    #     #=> 0

    # :method: touch
    # :call-seq:
    #   touch => Hash{Symbol => String,Integer}
    #
    # Sends command to touch job which extends the ttr. Returns the Beanstalkd response for
    # the command.
    #
    #   job = StalkClimber::Job.new(connection.transmit('reserve'))
    #   job.touch
    #     #=> {:status=>"TOUCHED", :body=>nil, :connection=>#<Beaneater::Connection host="localhost" port=11300>}

    # :method: ttr
    # :call-seq:
    #   ttr(force_refresh = false) => Integer
    #
    # Retrieves the time to run for the job, the number of seconds a worker is allowed to work
    # to run the job. Passing a non-false value for +force_refresh+ will force retrieval of
    # updated stats for the job By default the value for ttr is cached and will not be updated
    # unless forced.
    #
    #   job = StalkClimber::Job.new(connection.transmit('reserve'))
    #   job.ttr
    #     #=> 120

    # :method: tube
    # :call-seq:
    #   tube => String
    #
    # Retrieves the name of the tube that contains job. Passing a non-false value for
    # +force_refresh+ will force retrieval of updated stats for the job By default the
    # value for tube is cached and will not be updated unless forced.
    #
    #   job = StalkClimber::Job.new(connection.transmit('reserve'))
    #   job.tube
    #     #=> "stalk_climber"

  end
end
