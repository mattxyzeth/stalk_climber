module StalkClimber
  class Connection < Beaneater::Connection
    include RUBY_VERSION >= '2.0.0' ? LazyEnumerable : Enumerable

    DEFAULT_TUBE = 'stalk_climber'
    PROBE_TRANSMISSION = "put 4294967295 0 300 2\r\n{}"

    attr_reader :max_climbed_job_id, :min_climbed_job_id, :test_tube


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


    # Interface for job enumerator/enumeration in descending ID order. Returns an instance of
    # Job for each existing job on the beanstalk server. Jobs are enumerated in three phases. Jobs
    # between max_job_id and the max_climbed_job_id are pulled from beanstalk, cached, and yielded.
    # Jobs that have already been cached are yielded if they still exist, otherwise they are deleted
    # from the cache. Finally, jobs between min_climbed_job_id and 1 are pulled from beanstalk, cached,
    # and yielded.
    # Connection#each fulfills Enumberable contract, allowing connection to behave as an Enumerable.
    def climb
      enum = to_enum
      return enum unless block_given?
      loop do
        begin
          yield enum.next
        rescue StopIteration
          return ($ERROR_INFO.nil? || $ERROR_INFO.result.nil?) ? nil : $ERROR_INFO.result
        end
      end
    end
    alias_method :each, :climb


    # Safe form of fetch_job!, returns a Job instance for the specified +job_id+.
    # If the job does not exist, the error is caught and nil is passed returned instead.
    def fetch_job(job_id)
      begin
        job = fetch_job!(job_id)
      rescue Beaneater::NotFoundError
        job = nil
      end
      return job
    end


    # Returns a Job instance for the specified +job_id+. If the job does not exist,
    # a Beaneater::NotFoundError will bubble up from Beaneater. The job is not cached.
    def fetch_job!(job_id)
      return Job.new(transmit("peek #{job_id}"))
    end


    # Like fetch_job, but fetches all job ids in +job_ids+. Jobs are not cached and
    # nil is returned if any of the jobs don't exist.
    def fetch_jobs(*job_ids)
      return job_ids.flatten.map { |job_id| fetch_job(job_id) }
    end


    # Similar to fetch_job!, but fetches all job ids in +job_ids+. Jobs are not cached
    # and a Beaneater::NotFoundError is raised if any of the listed jobs don't exist.
    def fetch_jobs!(*job_ids)
      return job_ids.flatten.map { |job_id| fetch_job!(job_id) }
    end


    # Initializes a new Connection to the beanstalk +address+ provided and
    # configures the Connection to only use the configured test_tube for
    # all transmissions.
    # Optionally yields the instance if a block is given. The instance is yielded
    # prior to test_tube configuration to allow the test_tube to be configured.
    def initialize(address, test_tube = DEFAULT_TUBE)
      super(address)
      @test_tube = test_tube || DEFAULT_TUBE
      clear_cache
      yield(self) if block_given?
      use_test_tube
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


    # Set and use the provided +test_tube+
    def test_tube=(test_tube)
      @test_tube = test_tube
      use_test_tube
    end


    # Returns an Enumerator for crawling all existing jobs for a connection.
    # See Connection#each for more information.
    def to_enum
      return Enumerator.new do |yielder|
        max_id = max_job_id

        initial_cached_jobs = cache.values_at(*cache.keys.sort.reverse)

        max_id.downto(self.max_climbed_job_id + 1) do |job_id|
          job = fetch_and_cache_job(job_id)
          yielder << job unless job.nil?
        end

        initial_cached_jobs.each do |job|
          if job.exists?
            yielder << job
          else
            self.cache.delete(job.id)
          end
        end

        ([self.min_climbed_job_id - 1, max_id].min).downto(1) do |job_id|
          job = fetch_and_cache_job(job_id)
          yielder << job unless job.nil?
        end
      end
    end


    protected

    # Helper method, similar to fetch_job, that retrieves the job identified by
    # +job_id+, caches it, and updates counters before returning the job.
    # If the job does not exist, nothing is cached, however counters will be updated,
    # and nil is returned
    def fetch_and_cache_job(job_id)
      job = fetch_job(job_id)
      self.cache[job_id] = job unless job.nil?
      @min_climbed_job_id = job_id if job_id < @min_climbed_job_id
      @max_climbed_job_id = job_id if job_id > @max_climbed_job_id
      return job
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


    # Dispatch transmissions notifying Beanstalk to use the configured test_tube for all
    # commands from this connection and to ignore the default tube
    def use_test_tube
      [
        "use #{self.test_tube}",
        "watch #{self.test_tube}",
        'ignore default',
      ].each do |transmission|
        transmit(transmission)
      end
    end

  end
end
