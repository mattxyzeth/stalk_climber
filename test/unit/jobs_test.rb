require 'test_helper'

class JobsTest < Test::Unit::TestCase

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
    climber.jobs.each do |job|
      jobs[job.connection.address] ||= {}
      jobs[job.connection.address][job.id] = job
    end

    climber.expects(:with_job).never
    climber.jobs.each do |job|
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
      climber.jobs.each_threaded do |job|
        job
      end
    end

    climber.connection_pool.connections.each do |connection|
      test_jobs[connection.address].map(&:delete)
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
      climber.jobs.any? do |job|
        true
      end

      # test normal enumeration
      climber.jobs.all? do |job|
        job
      end
    end

    climber.connection_pool.connections.each do |connection|
      test_jobs[connection.address].map(&:delete)
    end
  end


end
