module StalkClimber
  class Tube < Beaneater::Tube

    # List of available attributes available via the stats object.
    STATS_ATTRIBUTES = %w[
      cmd-delete cmd-pause-tube current-jobs-buried current-jobs-delayed
      current-jobs-ready current-jobs-reserved current-jobs-urgent current-using
      current-waiting current-watching name pause pause-time-left total-jobs
    ]

    # Lookup table of which method to call to retrieve each stat
    STATS_METHODS = Hash[
      STATS_ATTRIBUTES.zip(STATS_ATTRIBUTES.map {|stat| stat.gsub(/-/, '_')})
    ]

    # Pause and name are available as stats but are not included in this list
    # because Beaneater::Tube already defines methods with those names. Instead,
    # the pause stat can be obtained via Tube#pause_time and name can be obtained
    # via the superclass method.
    STATS_METHODS.values.reject {|k,v| %w[name pause].include?(k)}.each do |method_name|
      define_method method_name do |force_refresh = true|
        return stats(force_refresh).send(method_name)
      end
    end


    # :call-seq:
    #   exists?() => Boolean
    #
    # Determines if a tube exists by retrieving stats for the tube. If Beaneater
    # can't find the tube then it oes not exist and false is returned. If stats
    # are retrieved successfully then true is returned. The stats command is used
    # because it will return a response of near constant size, whereas, depending
    # on the environment, list-tubes could return a much larger response.
    # Additionally, updated stats information is usually more valuable than a list
    # of tubes that is immediately discarded.
    def exists?
      return !stats.nil?
    rescue Beaneater::NotFoundError
      return false
    end


    # :call-seq:
    #   pause_time(force_refresh = false) => Integer
    #
    # Returns pause stat which indicates the duration the tube is currently
    # paused for. The name pause_time is used because Beaneater::Tube already
    # defines a pause method.
    def pause_time(force_refresh = false)
      return stats(force_refresh).pause
    end


    # :call-seq:
    #   stats(force_refresh = true) => Beaneater::StatStruct
    #
    # Returns related stats for this tube.
    #
    #   @tube.stats.current_jobs_delayed
    #     #=> 3
    def stats(force_refresh = true)
      if force_refresh || @stats.nil?
        stats = transmit_to_all("stats-tube #{name}", :merge => true)[:body]
        @stats = Beaneater::StatStruct.from_hash(stats)
      end
      return @stats
    end


    # :call-seq:
    #   to_h() => Hash
    #
    # Returns a hash of all tube attributes derived from updated stats
    #
    #   tube = StalkClimber::Tube.new(connection_pool, 'stalk_climber')
    #   tube.to_h
    #     #=> {"cmd-delete"=>100, "cmd-pause-tube"=>101, "current-jobs-buried"=>102, "current-jobs-delayed"=>103, "current-jobs-ready"=>104, "current-jobs-reserved"=>105, "current-jobs-urgent"=>106, "current-using"=>107, "current-waiting"=>108, "current-watching"=>109, "name"=>"stalk_climber", "pause"=>111, "pause-time-left"=>110, "total-jobs"=>112}
    def to_h
      stats
      return Hash[
        STATS_METHODS.map do |stat, method_name|
          [stat, stats(false).send(method_name)]
        end
      ]
    end


    # :call-seq:
    #   to_s() => String
    #
    # Returns string representation of the tube
    #
    #   tube = StalkClimber::Tube.new(connection_pool, 'stalk_climber')
    #   tube.to_s
    #     #=> #<StalkClimber::Tube name="stalk_climber">
    def to_s
      "#<StalkClimber::Tube name=#{name}>"
    end
    alias :inspect :to_s


    # :method: cmd_delete
    # :call-seq:
    #   cmd_delete() => Integer
    #
    # Returns the number of times the delete command has been called for
    # jobs in the tube.

    # :method: cmd_pause_tube
    # :call-seq:
    #   cmd_pause_tube() => Integer
    #
    # Returns the number of times the pause-tube command has been called on
    # the tube.

    # :method: current_jobs_buried
    # :call-seq:
    #   current_jobs_buried() => Integer
    #
    # Returns the number of jobs in the tube that are currently buried.

    # :method: current_jobs_delayed
    # :call-seq:
    #   current_jobs_buried() => Integer
    #
    # Returns the number of jobs in the tube that are currently buried.

    # :method: current_jobs_ready
    # :call-seq:
    #   current_jobs_ready() => Integer
    #
    # Returns the number of jobs in the tube that are currently ready.

    # :method: current_jobs_reserved
    # :call-seq:
    #   current_jobs_reserved() => Integer
    #
    # Returns the number of jobs in the tube that are currently reserved.

    # :method: current_jobs_urgent
    # :call-seq:
    #   current_jobs_urgent() => Integer
    #
    # Returns the number of jobs currently in the tube with a priority
    # less than 1024, and thus deemed urgent.

    # :method: current_using
    # :call-seq:
    #   current_using() => Integer
    #
    # Returns the number of connections that are currently using the tube.

    # :method: current_waiting
    # :call-seq:
    #   current_waiting() => Integer
    #
    # Returns the number of connections that have issued reserve requests
    # for the tube and are currently awaiting a reply.

    # :method: current_watching
    # :call-seq:
    #   current_watching() => Integer
    #
    # Returns the number of connections that are watching the tube.

    # :method: pause_time_left
    # :call-seq:
    #   pause_time_left() => Integer
    #
    # Returns the number of seconds remaining before the tube is no longer
    # paused.

    # :method: total_jobs
    # :call-seq:
    #   total_jobs() => Integer
    #
    # Returns the total number of jobs that have been registered with the tube
    # since the tube was most recently created.

  end
end
