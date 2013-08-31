module StalkClimber
  class ConnectionPool < Beaneater::Pool

    class InvalidURIScheme < RuntimeError; end

    attr_reader :addresses

    # Constructs a Beaneater::Pool from a less strict URL
    # +url+ can be a string i.e 'localhost:11300' or an array of addresses.
    def initialize(addresses = nil)
      @addresses = Array(parse_addresses(addresses) || host_from_env || Beaneater.configuration.beanstalkd_url)
      @connections = @addresses.map { |address| Connection.new(address) }
    end


    protected

    # Parses the given url into a collection of beanstalk addresses
    def parse_addresses(addresses)
      return if addresses.empty?
      uris = addresses.is_a?(Array) ? addresses.dup : addresses.split(/[\s,]+/)
      uris.map! do |uri_string|
        uri = URI.parse(uri_string)
        raise(InvalidURIScheme, "Invalid beanstalk URI: #{uri_string}") unless uri.scheme == 'beanstalk'
        "#{uri.host}:#{uri.port || 11300}"
      end
    end

  end
end
