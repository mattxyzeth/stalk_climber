module StalkClimber
  class Job < Beaneater::Job

    # Attributes that are retrieved each request by default
    FRESH_ATTRIBUTES = %w[delay pri state time-left]

    # Attributes that return cached values by default
    CACHED_ATTRIBUTES = %w[age buries kicks releases reserves timeouts ttr tube]

    # All attributes available via a job's stats
    STATS_ATTRIBUTES = (CACHED_ATTRIBUTES + FRESH_ATTRIBUTES).sort!

    # Attributes that should be included in a Hash representation of a job
    HASH_ATTRIBUTES = (STATS_ATTRIBUTES + %w[body connection id]).sort!

    STATS_ATTRIBUTES.each do |method_name|
      force = FRESH_ATTRIBUTES.include?(method_name)
      define_method method_name.gsub(/-/, '_') do |force_refresh = force|
        return stats(force_refresh)[method_name]
      end
    end


    # Returns or fetches the body of the job obtained via the peek command
    def body
      return @body ||= connection.transmit("peek #{id}")[:body]
    end


    # Deletes the job from beanstalk. If the job is not found it is assumed that it
    # has already been otherwise deleted.
    def delete
      result = super
      @status = 'DELETED'
      @stats = nil
      @body = nil
      return result
    end


    # Determines if a job exists by retrieving stats for the job. If Beaneater can't find
    # the job then it does not exist and false is returned. The stats command is used
    # because it will return a response of a near constant size, whereas, depending on
    # the job, the peek command could return a much larger response. Rather than waste
    # the trip to the server, stats are updated each time the method is called.
    def exists?
      return @status == 'DELETED' ? false : super
    end


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
        @stats = job_data[:body].dup
        @id = @stats.delete('id').to_i
        @body = nil
      else
        raise RuntimeError, "Unexpected job status: #{job_data[:status]}"
      end
      @status = job_data[:status]
      @connection = job_data[:connection]
      # Don't set @reserved to force lookup each time to handle job TTR expiration
    end


    # Returns or retrieves stats for the job. Optionally, a retrieve may be forced
    # by passing a non-false value for +force_refresh+
    def stats(force_refresh = true)
      if @stats.nil? || force_refresh
        @stats = connection.transmit("stats-job #{id}")[:body]
      end
      return Beaneater::StatStruct.from_hash(@stats)
    end


    # Returns a hash of all job attributes
    def to_h
      stats
      return Hash[HASH_ATTRIBUTES.map { |attr| [attr, @stats[attr] || send(attr)] } ]
    end


    # Returns string representation of job
    def to_s
      "#<StalkClimber::Job id=#{id} body=#{body.inspect}>"
    end
    alias :inspect :to_s


    # :method: age
    # Retrieves the age of the job from the job's stats. Passing a non-false value for
    # +force_refresh+ will force retrieval of updated stats for the job
    # :call-seq:
    #   age(force_refresh = false)

    # :method: buries
    # Retrieves the number of times the job has been buried from the job's stats. Passing
    # a non-false value for +force_refresh+ will force retrieval of updated stats for the job
    # :call-seq:
    #   buries(force_refresh = false)

    # :method: delay
    # Retrieves the the remaining delay before the job is ready from the job's stats. Passing
    # a non-false value for +force_refresh+ will force retrieval of updated stats for the job
    # :call-seq:
    #   delay(force_refresh = false)

    # :method: kicks
    # Retrieves the number of times the job has been kicked from the job's stats. Passing
    # a non-false value for +force_refresh+ will force retrieval of updated stats for the job
    # :call-seq:
    #   kicks(force_refresh = false)

    # :method: pri
    # Retrieves the priority of the job from the job's stats. Passing a non-false value for
    # +force_refresh+ will force retrieval of updated stats for the job
    # :call-seq:
    #   pri(force_refresh = false)

    # :method: releases
    # Retrieves the number of times the job has been released from the job's stats. Passing
    # a non-false value for +force_refresh+ will force retrieval of updated stats for the job
    # :call-seq:
    #   releases(force_refresh = false)

    # :method: reserves
    # Retrieves the number of times the job has been reserved from the job's stats. Passing
    # a non-false value for +force_refresh+ will force retrieval of updated stats for the job
    # :call-seq:
    #   reserves(force_refresh = false)

    # :method: state
    # Retrieves the state of the job from the job's stats. Value will be one of "ready",
    # "delayed", "reserved", or "buried". Passing a non-false value for +force_refresh+
    # will force retrieval of updated stats for the job
    # :call-seq:
    #   timeouts(force_refresh = false)

    # :method: time-left
    # Retrieves the number of seconds left until the server puts this job into the ready
    # queue. This number is only meaningful if the job is reserved or delayed. If the job
    # is reserved and this amount of time elapses before its state changes, it is considered
    # to have timed out. Passing a non-false value for +force_refresh+ will force retrieval
    # of updated stats for the job
    # :call-seq:
    #   timeouts(force_refresh = false)

    # :method: timeouts
    # Retrieves the number of times the job has timed out from the job's stats. Passing
    # a non-false value for +force_refresh+ will force retrieval of updated stats for the job
    # :call-seq:
    #   timeouts(force_refresh = false)

    # :method: ttr
    # Retrieves the time to run for the job, the number of seconds a worker is allowed to work
    # to run the job. Passing a non-false value for +force_refresh+ will force retrieval of
    # updated stats for the job
    # :call-seq:
    #   timeouts(force_refresh = false)

    # :method: tube
    # Retrieves the name of the tube that contains job. Passing a non-false value for
    # +force_refresh+ will force retrieval of updated stats for the job
    # :call-seq:
    #   timeouts(force_refresh = false)

  end
end
