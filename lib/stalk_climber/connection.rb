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


    def initialize(address)
      super
      self.test_tube = DEFAULT_TUBE
      @min_climbed_job_id = Float::INFINITY
      @max_climbed_job_id = 0
      yield(self) if block_given?
    end


    def job_exists?(job_id)
      begin
        transmit("stats-job #{job_id}")
        return true
      rescue Beaneater::NotFoundError
        return false
      end
    end


    def max_job_id
      [
        "use #{self.test_tube}",
        "watch #{self.test_tube}",
        'ignore default',
      ].each do |transmission|
        transmit(transmission)
      end
      id = transmit(PROBE_TRANSMISSION)[:id].to_i
      transmit("delete #{id}")
      @max_climbed_job_id = id if @max_climbed_job_id == id - 1
      return id
    end


    def with_job(job_id, &block)
      begin
        with_job!(job_id, &block)
      rescue Beaneater::NotFoundError
        block.call(nil)
      end
    end


    def with_job!(job_id, &block)
      job = transmit("peek #{job_id}")
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

      initial_cached_job_ids = cache.keys.sort.reverse

      max_id.downto(self.max_climbed_job_id + 1) do |job_id|
        cache_job_and_yield(job_id, &block)
      end

      initial_cached_job_ids.each do |job_id|
        if job_exists?(job_id)
          yield cache[job_id]
        else
          self.cache.delete(job_id)
        end
      end

      ([self.min_climbed_job_id - 1, max_id].min).downto(1) do |job_id|
        cache_job_and_yield(job_id, &block)
      end
      return
    end

    alias_method :each, :climb
    public :each

  end
end
