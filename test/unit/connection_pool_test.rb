# Tests adapted from Backburner::Connection tests
# https://github.com/nesquena/backburner/blob/master/test/connection_test.rb

require 'test_helper'

class ConnectionPoolTest < Test::Unit::TestCase

  def test_for_bad_url_it_should_raise_a_bad_url_error
    assert_raises(StalkClimber::ConnectionPool::InvalidURIScheme) do
      StalkClimber::ConnectionPool.new('fake://foo')
    end
  end


  def test_for_delegated_methods_it_should_delegate_methods_to_beanstalk_connection
    connection_pool = StalkClimber::ConnectionPool.new('beanstalk://localhost')
    assert_equal 'localhost', connection_pool.connections.first.host
  end


  def test_with_multiple_urls_it_should_support_array_of_connections
    connection_pool = StalkClimber::ConnectionPool.new(['beanstalk://localhost:11300','beanstalk://localhost'])
    connections = connection_pool.connections
    assert_equal 2, connection_pool.connections.size
    assert_equal ['localhost:11300','localhost:11300'], connections.map(&:address)
  end


  def test_with_multiple_urls_it_should_support_single_string_with_commas
    connection_pool = StalkClimber::ConnectionPool.new('beanstalk://localhost:11300,beanstalk://localhost')
    connections = connection_pool.connections
    assert_equal 2, connections.size
    assert_equal ['localhost:11300','localhost:11300'], connections.map(&:address)
  end


  def test_with_single_url_it_should_set_up_connection_pool
    connection_pool = StalkClimber::ConnectionPool.new('beanstalk://localhost')
    assert_kind_of StalkClimber::ConnectionPool, connection_pool
    assert_kind_of Beaneater::Pool, connection_pool
  end


  def test_with_a_single_url_it_should_convert_url_to_address_array
    connection_pool = StalkClimber::ConnectionPool.new('beanstalk://localhost')
    assert_equal ['localhost:11300'], connection_pool.addresses
  end


  def test_with_a_test_tube
    connection_pool = StalkClimber::ConnectionPool.new('beanstalk://localhost', 'test_tube')
    assert_equal 'test_tube', connection_pool.test_tube
    assert connection_pool.connections.all? { |connection| connection.test_tube == 'test_tube' }
  end

end
