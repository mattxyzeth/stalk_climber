require 'test_helper'

class Job < StalkClimber::TestCase

  setup do
    @connection = StalkClimber::Connection.new('localhost:11300')
    @job = seed_jobs(1).first
  end


  context 'beaneater integration' do

    should 'derive from Beaneater::Job' do
      assert_kind_of Beaneater::Job, @job
    end

  end


  context '#body' do

    should 'perform peek and set/return body' do
      body = "test #{Time.now.to_i}"

      @job.connection.expects(:transmit).returns({
        :body => body,
      })
      assert_equal body, @job.body

      @job.connection.expects(:transmit).never
      assert_equal body, @job.body
    end

  end


  context '#connection' do

    should 'set connection instance variable to connection' do
      assert @job.instance_variable_get(:@connection)
      assert @job.connection
    end

  end


  context '#delete' do

    should 'delete the job' do
      assert @job.delete
      assert_raises Beaneater::NotFoundError do
        @connection.transmit("peek #{@job.id}")
      end
      refute @job.instance_variable_get(:@body)
      refute @job.instance_variable_get(:@stats)
      assert_equal 'DELETED', @job.instance_variable_get(:@status)
    end

  end


  context '#exists?' do


    should 'verify the job exists' do
      assert @job.exists?

      @job.delete
      refute @job.exists?
    end

  end


  context '#initialize' do

    should 'support initialization with peek response' do
      job = StalkClimber::Job.new(@connection.transmit("peek #{@job.id}"))
      @connection.expects(:transmit).never
      assert_equal @connection, job.connection
      assert_equal @job.id, job.id
      assert_equal 'FOUND', job.instance_variable_get(:@status)
      assert_equal('{}', job.body)
      refute job.instance_variable_get(:@stats)
    end


    should 'support initialization with put response' do
      @connection.expects(:transmit).never
      assert @job.id
      assert_equal @connection, @job.connection
      assert_equal 'INSERTED', @job.instance_variable_get(:@status)
      refute @job.instance_variable_get(:@body)
      refute @job.instance_variable_get(:@stats)
    end


    should 'support initialization with stats response' do
      job = StalkClimber::Job.new(@connection.transmit("stats-job #{@job.id}"))
      @connection.expects(:transmit).never
      assert_equal @job.id, job.id
      assert_equal @connection, job.connection
      assert_equal 'OK', job.instance_variable_get(:@status)
      assert job.stats(false)
      assert job.instance_variable_get(:@stats)
      refute job.instance_variable_get(:@body)
      StalkClimber::Job::CACHED_ATTRIBUTES.each do |method_name|
        assert job.send(method_name)
      end
    end


    should 'support initialization with reserve response' do
      job = StalkClimber::Job.new(@connection.transmit('reserve'))
      @connection.expects(:transmit).never
      assert_equal @connection, job.connection
      assert job.id
      assert_equal 'RESERVED', job.instance_variable_get(:@status)
      assert_equal('{}', job.body)
      refute job.instance_variable_get(:@stats)
    end


    should 'raise error if initialized with unknown response type' do
      assert_raises RuntimeError do
        StalkClimber::Job.new({:status => 'DELETED'})
      end
    end

  end


  context 'stats methods' do

    should 'initialize stats attributes and attribute methods should return correct values' do
      stats_body = {
        'age' => 3,
        'buries' => 0,
        'delay' => 0,
        'id' => 4412,
        'kicks' => 0,
        'pri' => 4294967295,
        'releases' => 0,
        'reserves' => 0,
        'state' => 'ready',
        'time-left' => 0,
        'timeouts' => 0,
        'ttr'  => 300,
        'tube' => 'default',
      }
      stats_response = {
        :body => stats_body,
        :connection => @connection,
        :id => 149,
        :status => 'OK',
      }
      job = StalkClimber::Job.new(stats_response)
      StalkClimber::Job::CACHED_ATTRIBUTES.each do |method_name|
        assert_equal stats_body[method_name], job.send(method_name), "Expected #{stats_body[method_name.to_sym]} for #{method_name}, got #{job.send(method_name)}"
      end
    end


    should 'be able to force refresh of stats' do
      initial_value = @job.age
      @connection.expects(:transmit).returns({:body => {'age' => initial_value + 100}})
      assert_equal initial_value + 100, @job.age(true)
    end


    should 'allow for not refreshing stats' do
      initial_value = @job.age
      @connection.expects(:transmit).never
      assert_equal initial_value, @job.age(false)
    end

  end


  context '#stats' do

    should 'be able to force refresh' do
      body = {
        'age'=>3,
        'buries'=>0,
        'delay'=>0,
        'kicks'=>0,
        'id' => 4412,
        'pri'=>4294967295,
        'releases'=>0,
        'reserves'=>0,
        'state'=>'ready',
        'time-left'=>0,
        'timeouts'=>0,
        'ttr'=>300,
        'tube'=>'default',
      }
      stats_1 = {
        :body => {},
        :connection => @connection,
        :id => 149,
        :status => 'OK',
      }
      stats_2 = {
        :body => body,
        :connection => @connection,
        :id => 149,
        :status => 'OK',
      }
      @connection.expects(:transmit).twice.returns(stats_1, stats_2)
      job = StalkClimber::Job.new(@connection.transmit("stats-job #{@job.id}"))
      job.stats
      StalkClimber::Job::CACHED_ATTRIBUTES.each do |method_name|
        assert_equal body[method_name], job.send(method_name), "Expected #{body[method_name.to_sym]} for #{method_name}, got #{job.send(method_name)}"
      end
    end


    should 'allow for not refreshing stats' do
      stat = @job.stats
      @job.connection.expects(:transmit).never
      assert_equal stat, @job.stats(false)
    end

  end


  context '#to_h' do

    should 'return the expected hash' do
      job_body = "test #{Time.now.to_i}"
      stats_body = {
        'age' => 3,
        'body' => job_body, # Will be ignored during job init
        'buries' => 0,
        'connection' => @connection, # Will be ignored during job init
        'delay' => 0,
        'id' => 4412,
        'kicks' => 0,
        'pri' => 4294967295,
        'releases' => 0,
        'reserves' => 0,
        'state' => 'ready',
        'time-left' => 0,
        'timeouts' => 0,
        'ttr'  => 300,
        'tube' => 'default',
      }
      stats_response = {
        :body => stats_body,
        :connection => @connection,
        :id => 149,
        :status => 'OK',
      }
      job = StalkClimber::Job.new(stats_response)
      job.instance_variable_set(:@body, job_body)
      job.connection.expects(:transmit).once.returns(stats_response)
      expected_hash = Hash[stats_body.map { |k, v| [k, v] }]
      assert_equal expected_hash, job.to_h
    end

  end


  context '#to_s' do

    should 'return expected string representation of job' do
      assert_equal "#<StalkClimber::Job id=#{@job.id} body=#{@job.body.inspect}>", @job.to_s
    end

  end

end
