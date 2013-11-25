require 'test_helper'

class JobsTest < StalkClimber::TestCase

  context '#each' do

    should 'cache jobs for later use' do
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

  end


  context '#each_threaded' do

    should 'work correctly in non-break situations' do
      climber = StalkClimber::Climber.new(BEANSTALK_ADDRESSES)
      test_jobs = {}
      climber.connection_pool.connections.each do |connection|
        test_jobs[connection.address] = []
        5.times.to_a.map! do
            test_jobs[connection.address] << StalkClimber::Job.new(connection.transmit(StalkClimber::Connection::PROBE_TRANSMISSION))
        end
      end

      climber.jobs.each_threaded do |job|
        job
      end

      climber.connection_pool.connections.each do |connection|
        test_jobs[connection.address].map(&:delete)
      end
    end

  end


  context 'enumberable contract' do

    should 'function correctly as an enumerable' do
      climber = StalkClimber::Climber.new(BEANSTALK_ADDRESSES)
      test_jobs = {}
      climber.connection_pool.connections.each do |connection|
        test_jobs[connection.address] = []
        5.times.to_a.map! do
            test_jobs[connection.address] << StalkClimber::Job.new(connection.transmit(StalkClimber::Connection::PROBE_TRANSMISSION))
        end
      end

      # verify enumeration can be short circuited
      climber.jobs.any? do |job|
        true
      end

      # test normal enumeration
      climber.jobs.all? do |job|
        job
      end

      climber.connection_pool.connections.each do |connection|
        test_jobs[connection.address].map(&:delete)
      end
    end

  end

end
