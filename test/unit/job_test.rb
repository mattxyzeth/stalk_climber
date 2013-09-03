require 'test_helper'

class Job < Test::Unit::TestCase

  def setup
    @connection = StalkClimber::Connection.new('localhost:11300')
    @job = seed_jobs(1).first
  end


  def test_body_retrives_performs_peek
    body = {:test => true}

    @job.connection.expects(:transmit).returns({
      :body => body,
    })
    assert_equal body, @job.body

    @job.connection.expects(:transmit).never
    assert_equal body, @job.body
  end


  def test_connection_stored_with_instance
    assert @job.instance_variable_get(:@connection)
    assert @job.connection
  end


  def test_delete
    assert @job.delete
    assert_raise Beaneater::NotFoundError do
      @connection.transmit("peek #{@job.id}")
    end
    refute @job.instance_variable_get(:@body)
    refute @job.instance_variable_get(:@stats)
    assert_equal 'DELETED', @job.instance_variable_get(:@status)
  end


  def test_exists?
    assert @job.exists?

    @job.delete
    refute @job.exists?
  end


  def test_initialize_with_peek_response
    job = StalkClimber::Job.new(@connection.transmit("peek #{@job.id}"))
    @connection.expects(:transmit).never
    assert_equal @connection, job.connection
    assert_equal @job.id, job.id
    assert_equal 'FOUND', job.instance_variable_get(:@status)
    assert_equal '{}', job.body
    refute job.instance_variable_get(:@stats)
  end


  def test_initialize_with_put_response
    @connection.expects(:transmit).never
    assert @job.id
    assert_equal @connection, @job.connection
    assert_equal 'INSERTED', @job.instance_variable_get(:@status)
    refute @job.instance_variable_get(:@body)
    refute @job.instance_variable_get(:@stats)
  end


  def test_initialize_with_stats_response
    job = StalkClimber::Job.new(@connection.transmit("stats-job #{@job.id}"))
    @connection.expects(:transmit).never
    assert_equal @job.id, job.id
    assert_equal @connection, job.connection
    assert_equal 'OK', job.instance_variable_get(:@status)
    assert job.stats
    assert job.instance_variable_get(:@stats)
    refute job.instance_variable_get(:@body)
    StalkClimber::Job::STATS_ATTRIBUTES.each do |method_name|
      assert job.send(method_name)
    end
  end


  def test_initialize_with_stats_attribute_methods_return_correct_values
    body = {
      'age'=>3,
      'buries'=>0,
      'delay'=>0,
      'id' => 4412,
      'kicks'=>0,
      'pri'=>4294967295,
      'releases'=>0,
      'reserves'=>0,
      'state'=>'ready',
      'time-left'=>0,
      'timeouts'=>0,
      'ttr'=>300,
      'tube'=>'default',
    }
    stats = {
      :body => body,
      :connection => @connection,
      :id => 149,
      :status => 'OK',
    }
    @connection.expects(:transmit).returns(stats)
    job = StalkClimber::Job.new(@connection.transmit("stats-job #{@job.id}"))
    @connection.expects(:transmit).never
    StalkClimber::Job::STATS_ATTRIBUTES.each do |method_name|
      assert_equal body[method_name], job.send(method_name), "Expected #{body[method_name.to_sym]} for #{method_name}, got #{job.send(method_name)}"
    end
  end


  def test_initialize_with_unknown_status_raises_error
    assert_raise RuntimeError do
      StalkClimber::Job.new({:status => 'DELETED'})
    end
  end


  def test_stats_attribute_method_can_force_refresh
    initial_value = @job.age
    @connection.expects(:transmit).returns({:body => {'age' => initial_value + 100}})
    assert_equal initial_value + 100, @job.age(true)
  end


  def test_stats_can_force_a_refresh
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
    job.stats(true)
    StalkClimber::Job::STATS_ATTRIBUTES.each do |method_name|
      assert_equal body[method_name], job.send(method_name), "Expected #{body[method_name.to_sym]} for #{method_name}, got #{job.send(method_name)}"
    end
  end

end
