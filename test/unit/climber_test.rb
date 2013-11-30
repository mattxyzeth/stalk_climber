require 'test_helper'

class ClimberTest < StalkClimber::TestCase

  context '#connection_pool' do

    should 'create a ConnectionPool' do
      climber = StalkClimber::Climber.new('beanstalk://localhost')
      assert_kind_of StalkClimber::ConnectionPool, climber.connection_pool
    end


    should 'raise an error without beanstalk addresses' do
      climber = StalkClimber::Climber.new
      assert_raises RuntimeError do
        climber.connection_pool
      end
    end

  end


  context 'max_job_ids' do

    should 'return correct max job ids' do
      climber = StalkClimber::Climber.new(BEANSTALK_ADDRESSES, 'test_tube')
      max_ids = climber.max_job_ids
      max_ids.each do |connection, max_id|
        assert_equal connection.max_job_id - 1, max_id
      end
    end

  end


  context '#jobs' do

    should 'return an instance of Jobs' do
      assert_kind_of StalkClimber::Jobs, StalkClimber::Climber.new(BEANSTALK_ADDRESSES).jobs
    end

  end


  context '#initialize' do

    should 'accept a custom test tube' do
      climber = StalkClimber::Climber.new(BEANSTALK_ADDRESSES, 'test_tube')
      assert_equal 'test_tube', climber.test_tube
      assert_equal 'test_tube', climber.connection_pool.test_tube
    end

  end


  context '#tubes' do

    should 'delegate to connection_pool for tubes' do
      climber = StalkClimber::Climber.new(BEANSTALK_ADDRESSES, 'test_tube')
      connection_pool = climber.connection_pool
      connection_pool.expects(:tubes).once
      climber.tubes
    end

  end

end
