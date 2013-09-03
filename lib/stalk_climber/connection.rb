module StalkClimber
  class Connection < Beaneater::Connection
    include LazyEnumerable

    DEFAULT_TUBE = 'stalk_climber'
    PROBE_TRANSMISSION = "put 4294967295 0 300 2\r\n{}"

    attr_accessor :test_tube
    attr_reader :max_climbed_job_id, :min_climbed_job_id


    def cache
      return @cache ||= {}
    end


    def clear_cache
      @cache = nil
      @min_climbed_job_id = Float::INFINITY
      @max_climbed_job_id = 0
    end


    def initialize(address)
      super
      self.test_tube = DEFAULT_TUBE
      clear_cache
      yield(self) if block_given?
    end


    def max_job_id
      [
        "use #{self.test_tube}",
        "watch #{self.test_tube}",
        'ignore default',
      ].each do |transmission|
        transmit(transmission)
      end
      job = Job.new(transmit(PROBE_TRANSMISSION))
      job.delete
      update_climbed_job_ids_from_max_id(job.id)
      return job.id
    end


    def with_job(job_id, &block)
      begin
        with_job!(job_id, &block)
      rescue Beaneater::NotFoundError
        block.call(nil)
      end
    end


    def with_job!(job_id, &block)
      job = Job.new(transmit("peek #{job_id}"))
      block.call(job)
    end


    protected

    def cache_job_and_yield(job_id, &block)
      with_job(job_id) do |job|
        self.cache[job_id] = job unless job.nil?
        @min_climbed_job_id = job_id if job_id < @min_climbed_job_id
        @max_climbed_job_id = job_id if job_id > @max_climbed_job_id
        yield(job) unless job.nil?
      end
    end


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

    alias_method :each, :climb
    public :each


    def update_climbed_job_ids_from_max_id(new_max_id)
      if @max_climbed_job_id > 0 && @max_climbed_job_id == new_max_id - 1
        @max_climbed_job_id = new_max_id
      elsif new_max_id < @max_climbed_job_id
       # In case server reset since last climb
        clear_cache
      end
    end

  end
end
