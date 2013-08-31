require 'test_helper'

class ClimberTest < Test::Unit::TestCase

  def test_climb_caches_jobs_for_later_use
    climber = StalkClimber::Climber.new do |c|
      c.beanstalk_addresses = 'beanstalk://localhost'
    end

    connection = climber.connection_pool.connections.first
    job_ids = 5.times.to_a.map! do
        connection.transmit(StalkClimber::Connection::PROBE_TRANSMISSION)[:id]
    end

    jobs = {}
    climber.each do |job|
      jobs[job[:connection].address] ||= {}
      jobs[job[:connection].address][job[:id]] = job
    end

    climber.expects(:with_job).never
    climber.each do |job|
      assert_equal jobs[job[:connection].address][job[:id]], job
    end
    job_ids.each do |job_id|
      connection.transmit("delete #{job_id}")
    end
  end


  def test_connection_pool_creates_a_connection_pool
    climber = StalkClimber::Climber.new do |c|
      c.beanstalk_addresses = 'beanstalk://localhost'
    end
    assert_kind_of StalkClimber::ConnectionPool, climber.connection_pool
  end


  def test_connection_pool_raises_an_error_without_beanstalk_addresses
    climber = StalkClimber::Climber.new
    assert_raise RuntimeError do
      climber.connection_pool
    end
  end


  def test_each_is_an_alias_for_climb
    assert_equal(
      StalkClimber::Climber.instance_method(:climb),
      StalkClimber::Climber.instance_method(:each),
      'Expected StalkClimber::Climber#each to be an alias for StalkClimber::Climber#climb'
    )
  end

end
