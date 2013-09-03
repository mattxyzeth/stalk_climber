module StalkClimber
  class Connection < Beaneater::Connection
    include LazyEnumerable

    DEFAULT_TUBE = 'stalk_climber'
    PROBE_TRANSMISSION = "put 4294967295 0 300 2\r\n{}"

    attr_accessor :test_tube
    attr_reader :max_climbed_job_id, :min_climbed_job_id


    # Returns or creates a Hash used for caching jobs by ID
    def cache
      return @cache ||= {}
    end


    # Resets the job cache and reinitializes the min and max climbed job ids
    def clear_cache
      @cache = nil
      @min_climbed_job_id = Float::INFINITY
      @max_climbed_job_id = 0
    end


    # Handles job enumeration in descending ID order, passing an instance of Job to +block+ for
    # each existing job on the beanstalk server. Jobs are enumerated in three phases. Jobs between
    # max_job_id and the max_climbed_job_id are pulled from beanstalk, cached, and given to
    # +block+. Jobs that have already been cached are yielded to +block+ if they still exist,
    # otherwise they are deleted from the cache. Finally, jobs between min_climbed_job_id and 1
    # are pulled from beanstalk, cached, and given to +block+.
    # Connection#each fulfills Enumberable contract, allowing connection to behave as an Enumerable.
    def each(&block)
      climb(&block)
    end


    # Initializes a new Connection to the beanstalk +address+ provided and
    # configures the Connection to only use the configured test_tube for
    # all transmissions.
    # Optionally yields the instance if a block is given. The instance is yielded
    # prior to test_tube configuration to allow the test_tube to be configured.
    def initialize(address)
      super
      self.test_tube = DEFAULT_TUBE
      clear_cache
      yield(self) if block_given?
      [
        "use #{self.test_tube}",
        "watch #{self.test_tube}",
        'ignore default',
      ].each do |transmission|
        transmit(transmission)
      end
    end


    # Determintes the max job ID of the connection by inserting a job into the test tube
    # and immediately deleting it. Before returning the max ID, the max ID is used to
    # update the max_climbed_job_id (if sequentual) and possibly invalidate the cache.
    # The cache will be invalidated if the max ID is less than any known IDs since
    # new job IDs should always increment unless there's been a change in server state.
    def max_job_id
      job = Job.new(transmit(PROBE_TRANSMISSION))
      job.delete
      update_climbed_job_ids_from_max_id(job.id)
      return job.id
    end


    # Safe form of with_job!, yields a Job instance to +block+ for the specified +job_id+.
    # If the job does not exist, the error is caught and nil is passed to +block+ instead.
    def with_job(job_id, &block)
      begin
        with_job!(job_id, &block)
      rescue Beaneater::NotFoundError
        block.call(nil)
      end
    end


    # Yields a Job instance to +block+ for the specified +job_id+.
    # If the job does not exist, a Beaneater::NotFoundError will bubble up from Beaneater.
    def with_job!(job_id, &block)
      job = Job.new(transmit("peek #{job_id}"))
      block.call(job)
    end


    protected

    # Helper method, similar to with_job, that retrieves the job identified by
    # +job_id+, caches it, and  updates counters before yielding the job.
    # If the job does not exist, +block+ is not called and nothing is cached,
    # however counters will be updated.
    def cache_job_and_yield(job_id, &block)
      with_job(job_id) do |job|
        self.cache[job_id] = job unless job.nil?
        @min_climbed_job_id = job_id if job_id < @min_climbed_job_id
        @max_climbed_job_id = job_id if job_id > @max_climbed_job_id
        yield(job) unless job.nil?
      end
    end


    # Handles job enumeration. See Connection#each for more information.
    def climb(&block)
      max_id = max_job_id

      initial_cached_jobs = cache.values_at(*cache.keys.sort.reverse)

      max_id.downto(self.max_climbed_job_id + 1) do |job_id|
        cache_job_and_yield(job_id, &block)
      end

      initial_cached_jobs.each do |job|
        if job.exists?
          yield job
        else
          self.cache.delete(job.id)
        end
      end

      ([self.min_climbed_job_id - 1, max_id].min).downto(1) do |job_id|
        cache_job_and_yield(job_id, &block)
      end
      return
    end


    # Uses +new_max_id+ to update the max_climbed_job_id (if sequentual) and possibly invalidate
    # the cache. The cache will be invalidated if +new_max_id+ is less than any known IDs since
    # new job IDs should always increment unless there's been a change in server state.
    def update_climbed_job_ids_from_max_id(new_max_id)
      if @max_climbed_job_id > 0 && @max_climbed_job_id == new_max_id - 1
        @max_climbed_job_id = new_max_id
      elsif new_max_id < @max_climbed_job_id
        clear_cache
      end
    end

  end
end
