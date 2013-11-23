require 'test_helper'

class ClimberTest < Test::Unit::TestCase

  def test_each_caches_jobs_for_later_use
    climber = StalkClimber::Climber.new(BEANSTALK_ADDRESSES)

    test_jobs = {}
    climber.connection_pool.connections.each do |connection|
      test_jobs[connection.address] = []
      5.times.to_a.map! do
          test_jobs[connection.address] << StalkClimber::Job.new(connection.transmit(StalkClimber::Connection::PROBE_TRANSMISSION))
      end
    end

    jobs = {}
    climber.each do |job|
      jobs[job.connection.address] ||= {}
      jobs[job.connection.address][job.id] = job
    end

    climber.expects(:with_job).never
    climber.each do |job|
      assert_equal jobs[job.connection.address][job.id], job
    end

    climber.connection_pool.connections.each do |connection|
      test_jobs[connection.address].map(&:delete)
    end
  end


  def test_each_threaded_works_for_non_break_situation
    climber = StalkClimber::Climber.new(BEANSTALK_ADDRESSES)
    test_jobs = {}
    climber.connection_pool.connections.each do |connection|
      test_jobs[connection.address] = []
      5.times.to_a.map! do
          test_jobs[connection.address] << StalkClimber::Job.new(connection.transmit(StalkClimber::Connection::PROBE_TRANSMISSION))
      end
    end

    assert_nothing_raised do
      # test normal enumeration
      climber.each_threaded do |job|
        job
      end
    end

    climber.connection_pool.connections.each do |connection|
      test_jobs[connection.address].map(&:delete)
    end
  end


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


  def test_enumerable_works_correctly
    climber = StalkClimber::Climber.new(BEANSTALK_ADDRESSES)
    test_jobs = {}
    climber.connection_pool.connections.each do |connection|
      test_jobs[connection.address] = []
      5.times.to_a.map! do
          test_jobs[connection.address] << StalkClimber::Job.new(connection.transmit(StalkClimber::Connection::PROBE_TRANSMISSION))
      end
    end

    assert_nothing_raised do
      # verify enumeration can be short circuited
      climber.any? do |job|
        true
      end

      # test normal enumeration
      climber.all? do |job|
        job
      end
    end

    climber.connection_pool.connections.each do |connection|
      test_jobs[connection.address].map(&:delete)
    end
  end


  def test_each_is_an_alias_for_climb
    assert_equal(
      StalkClimber::Climber.instance_method(:climb),
      StalkClimber::Climber.instance_method(:each),
      'Expected StalkClimber::Climber#each to be an alias for StalkClimber::Climber#climb'
    )
  end


  def test_each_threaded_is_an_alias_for_climb_threaded
    assert_equal(
      StalkClimber::Climber.instance_method(:climb_threaded),
      StalkClimber::Climber.instance_method(:each_threaded),
      'Expected StalkClimber::Climber#each_threaded to be an alias for StalkClimber::Climber#climb_threaded'
    )
  end


  def test_max_job_ids_returns_the_correct_max_job_ids
    climber = StalkClimber::Climber.new(BEANSTALK_ADDRESSES, 'test_tube')
    max_ids = climber.max_job_ids
    max_ids.each do |connection, max_id|
      assert_equal connection.max_job_id - 1, max_id
    end
  end


  def test_to_enum_returns_an_enumerator
    assert_kind_of Enumerator, StalkClimber::Climber.new(BEANSTALK_ADDRESSES).to_enum
  end


  def test_with_a_test_tube
    climber = StalkClimber::Climber.new(BEANSTALK_ADDRESSES, 'test_tube')
    assert_equal 'test_tube', climber.test_tube
    assert_equal 'test_tube', climber.connection_pool.test_tube
  end

end
