module StalkClimber
  class Job

    STATS_ATTRIBUTES = %w[age buries delay kicks pri releases reserves state time-left timeouts ttr tube]
    attr_reader :id

    STATS_ATTRIBUTES.each do |method_name|
      define_method method_name do |force_refresh = false|
        return stats(force_refresh)[method_name]
      end
    end


    def body
      return @body ||= connection.transmit("peek #{id}")[:body]
    end


    def connection
      return @connection
    end


    def delete
      begin
        @connection.transmit("delete #{id}")
      rescue Beaneater::NotFoundError
      end
      @status = 'DELETED'
      @stats = nil
      @body = nil
      return true
    end


    def exists?
      begin
        stats(true)
        return true
      rescue Beaneater::NotFoundError
        return false
      end
    end


    def initialize(job_data)
      case job_data[:status]
      when 'INSERTED' # put
        @id = job_data[:id].to_i
        @body = @stats = nil
      when 'FOUND' # peek
        @id = job_data[:id].to_i
        @body = job_data[:body]
        @stats = nil
      when 'OK' # stats-job
        @body = nil
        @stats = job_data.delete(:body)
        @id = @stats.delete('id').to_i
      else
        raise RuntimeError, "Unexpected job status: #{job_data[:status]}"
      end
      @status = job_data[:status]
      @connection = job_data[:connection]
    end


    def stats(force_refresh = false)
      return @stats unless @stats.nil? || force_refresh
      @stats = connection.transmit("stats-job #{id}")[:body]
      @stats.delete('id')
      return @stats
    end

  end
end
