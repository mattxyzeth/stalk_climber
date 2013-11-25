# Tests adapted from Backburner::Connection tests
# https://github.com/nesquena/backburner/blob/master/test/connection_test.rb

require 'test_helper'

class ConnectionPoolTest < StalkClimber::TestCase

  context '#initialize' do

    should 'test for bad url and raise StalkClimber::ConnectionPool::InvalidURIScheme' do
      assert_raises(StalkClimber::ConnectionPool::InvalidURIScheme) do
        StalkClimber::ConnectionPool.new('fake://foo')
      end
    end


    should 'support multiple connections when passed as an array' do
      connection_pool = StalkClimber::ConnectionPool.new(['beanstalk://localhost:11300','beanstalk://localhost'])
      connections = connection_pool.connections
      assert_equal 2, connection_pool.connections.size
      assert_equal ['localhost:11300','localhost:11300'], connections.map(&:address)
    end


    should 'support multiple connections when passed as a comma separated string' do
      connection_pool = StalkClimber::ConnectionPool.new('beanstalk://localhost:11300,beanstalk://localhost')
      connections = connection_pool.connections
      assert_equal 2, connections.size
      assert_equal ['localhost:11300','localhost:11300'], connections.map(&:address)
    end


    should 'support a single connection url passed as a string' do
      connection_pool = StalkClimber::ConnectionPool.new('beanstalk://localhost')
      assert_kind_of StalkClimber::ConnectionPool, connection_pool
      assert_kind_of Beaneater::Pool, connection_pool
    end


    should 'convert a single connection url into an array' do
      connection_pool = StalkClimber::ConnectionPool.new('beanstalk://localhost')
      assert_equal ['localhost:11300'], connection_pool.addresses
    end


    should 'support using a custom test tube' do
      connection_pool = StalkClimber::ConnectionPool.new('beanstalk://localhost', 'test_tube')
      assert_equal 'test_tube', connection_pool.test_tube
      assert connection_pool.connections.all? { |connection| connection.test_tube == 'test_tube' }
    end
  end


  context 'delegations' do

    should 'delegate methods to beanstalk connection' do
      connection_pool = StalkClimber::ConnectionPool.new('beanstalk://localhost')
      assert_equal 'localhost', connection_pool.connections.first.host
    end

  end

end
