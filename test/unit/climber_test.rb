require 'test_helper'

class ClimberTest < Test::Unit::TestCase

  def test_connection_pool_creates_a_connection_pool
    climber = StalkClimber::Climber.new('beanstalk://localhost')
    assert_kind_of StalkClimber::ConnectionPool, climber.connection_pool
  end


  def test_connection_pool_raises_an_error_without_beanstalk_addresses
    climber = StalkClimber::Climber.new
    assert_raise RuntimeError do
      climber.connection_pool
    end
  end


  def test_max_job_ids_returns_the_correct_max_job_ids
    climber = StalkClimber::Climber.new(BEANSTALK_ADDRESSES, 'test_tube')
    max_ids = climber.max_job_ids
    max_ids.each do |connection, max_id|
      assert_equal connection.max_job_id - 1, max_id
    end
  end


  def test_jobs_returns_an_instance_of_jobs_class
    assert_kind_of StalkClimber::Jobs, StalkClimber::Climber.new(BEANSTALK_ADDRESSES).jobs
  end


  def test_with_a_test_tube
    climber = StalkClimber::Climber.new(BEANSTALK_ADDRESSES, 'test_tube')
    assert_equal 'test_tube', climber.test_tube
    assert_equal 'test_tube', climber.connection_pool.test_tube
  end

end
