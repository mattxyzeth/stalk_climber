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
    connection = StalkClimber::ConnectionPool.new('beanstalk://localhost')
    assert_equal 'localhost', connection.connections.first.host
  end


  def test_with_multiple_urls_it_should_support_array_of_connections
    connection = StalkClimber::ConnectionPool.new(['beanstalk://localhost:11300','beanstalk://localhost'])
    connections = connection.connections
    assert_equal 2, connection.connections.size
    assert_equal ['localhost:11300','localhost:11300'], connections.map(&:address)
  end


  def test_with_multiple_urls_it_should_support_single_string_with_commas
    connection = StalkClimber::ConnectionPool.new('beanstalk://localhost:11300,beanstalk://localhost')
    connections = connection.connections
    assert_equal 2, connections.size
    assert_equal ['localhost:11300','localhost:11300'], connections.map(&:address)
  end


  def test_with_single_url_it_should_set_up_connection_pool
    connection = StalkClimber::ConnectionPool.new('beanstalk://localhost')
    assert_kind_of StalkClimber::ConnectionPool, connection
    assert_kind_of Beaneater::Pool, connection
  end


  def test_with_a_single_url_it_should_convert_url_to_address_array
    connection = StalkClimber::ConnectionPool.new('beanstalk://localhost')
    assert_equal ['localhost:11300'], connection.addresses
  end

end
