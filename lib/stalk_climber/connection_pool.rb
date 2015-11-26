module StalkClimber
  class ConnectionPool < Beaneater

    class InvalidURIScheme < RuntimeError; end

    # Addresses the pool is connected to
    attr_reader :addresses

    # Test tube used when probing Beanstalk server for information
    attr_reader :test_tube

    # Constructs a Beaneater::Pool from a less strict URL +url+ can be a string
    # or an array of addresses. Valid URLs match any of the following forms:
    #   localhost (host only)
    #   192.168.1.100:11300 (host and port)
    #   beanstalk://127.0.0.1:11300 (host and port prefixed by beanstalk scheme)
    def initialize(addresses = nil, test_tube = nil)
      @addresses = Array(parse_addresses(addresses) || host_from_env || Beaneater.configuration.beanstalkd_url)
      @test_tube = test_tube
      @connections = @addresses.map { |address| Connection.new(address, test_tube) }
    end


    def tubes
      @tubes ||= StalkClimber::Tubes.new(self)
    end


    protected

    # :call-seq:
    #   parse_addresses(addresses) => String
    #
    # Parses the given urls into a collection of beanstalk addresses
    def parse_addresses(addresses)
      return if addresses.empty?
      uris = addresses.is_a?(Array) ? addresses.dup : addresses.split(/[\s,]+/)
      uris.map! do |uri_string|
        begin
          uri = URI.parse(uri_string)
        rescue URI::InvalidURIError
          # IP based hosts without a scheme will fail to parse
        end
        if uri && uri.scheme && uri.host
          raise(InvalidURIScheme, "Invalid beanstalk URI: #{uri_string}") unless uri.scheme == 'beanstalk'
          host = uri.host
          port = uri.port
        else
          # if parse failure or missing scheme or host, assume the uri format is a
          # hostname optionally followed by a port.
          # i.e. 'localhost:11300' or '0.0.0.0'
          match = uri_string.split(/:/)
          host = match[0]
          port = match[1]
        end
        "#{host}:#{port || 11300}"
      end
    end

  end
end
