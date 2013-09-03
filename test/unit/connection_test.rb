require 'test_helper'

class ConnectionTest < Test::Unit::TestCase

  def setup
    @connection = StalkClimber::Connection.new('localhost:11300')
  end


  def test_cache_creates_and_returns_hash_instance_variable
    refute @connection.instance_variable_get(:@cache)
    assert_equal({}, @connection.cache)
    assert_equal({}, @connection.instance_variable_get(:@cache))
  end


  def test_cache_is_reset_if_max_job_id_lower_than_max_climbed_job_id
    seeds = seed_jobs
    @connection.each {}
    assert_not_equal({}, @connection.cache)
    assert_not_equal(Float::INFINITY, @connection.min_climbed_job_id)
    assert_not_equal(0, @connection.max_climbed_job_id)

    @connection.send(:update_climbed_job_ids_from_max_id, 1)
    assert_equal({}, @connection.cache)
    assert_equal(Float::INFINITY, @connection.min_climbed_job_id)
    assert_equal(0, @connection.max_climbed_job_id)
    seeds.map(&:delete)
  end


  def test_connection_is_some_kind_of_enumerable
    assert @connection.kind_of?(Enumerable)
  end


  def test_each_caches_jobs_for_later_use
    seeds = seed_jobs

    jobs = {}
    @connection.each do |job|
      jobs[job.id] = job
    end

    @connection.expects(:with_job).never
    @connection.each do |job|
      assert_equal jobs[job.id], job
    end

    seeds.map(&:delete)
  end


  def test_each_deletes_cached_jobs_that_no_longer_exist
    seeds = seed_jobs

    jobs = {}
    @connection.each do |job|
      jobs[job.id] = job
    end

    deleted_job = jobs[jobs.keys[2]]
    deleted_job.delete

    @connection.expects(:with_job).never
    @connection.each do |job|
      assert_equal jobs.delete(job.id), job
    end

    assert_equal 1, jobs.length
    assert_equal deleted_job.id, jobs.values.first.id

    seeds.map(&:delete)
  end


  def test_each_hits_jobs_below_climbed_range_that_have_not_been_hit
    seeds = seed_jobs(10)

    count = 0
    @connection.each do |job|
      count += 1
      break if count == 5
    end

    initial_min_climbed_job_id = @connection.min_climbed_job_id

    all_jobs = {}
    @connection.each do |job|
      all_jobs[job.id] = job
    end

    seeds.each do |job|
      assert_equal all_jobs[job.id].body, job.body
      assert_equal all_jobs[job.id].id, job.id
      job.delete
    end

    assert @connection.min_climbed_job_id < initial_min_climbed_job_id
  end


  def test_each_only_hits_each_job_once
    seeds = seed_jobs

    jobs = {}
    @connection.each do |job|
      refute jobs[job.id]
      jobs[job.id] = job
    end

    seeds.map(&:delete)
  end


  def test_each_is_an_alias_for_climb
    assert_equal(
      StalkClimber::Connection.instance_method(:climb),
      StalkClimber::Connection.instance_method(:each),
      'Expected StalkClimber::Connection#each to be an alias for StalkClimber::Connection#climb'
    )
  end


  def test_max_job_id_returns_expected_max_job_id
    initial_max = @connection.max_job_id
    seed_jobs(3).map(&:delete)
    # 3 new jobs, +1 for the probe job
    assert_equal initial_max + 4, @connection.max_job_id
  end


  def test_max_job_id_should_increment_max_climbed_id_if_successor
    @connection.each {}
    max = @connection.max_climbed_job_id
    @connection.max_job_id
    assert_equal max + 1, @connection.max_climbed_job_id
  end


  def test_max_job_id_should_not_increment_max_climbed_id_unless_successor
    @connection.each {}
    max = @connection.max_climbed_job_id
    seed_jobs(1).map(&:delete)
    @connection.max_job_id
    assert_equal max, @connection.max_climbed_job_id
  end


  def test_each_sets_min_and_max_climbed_job_ids_appropriately
    assert_equal Float::INFINITY, @connection.min_climbed_job_id
    assert_equal 0, @connection.max_climbed_job_id

    max_id = @connection.max_job_id
    @connection.expects(:max_job_id).once.returns(max_id)
    @connection.each {}

    assert_equal 1, @connection.min_climbed_job_id
    assert_equal max_id, @connection.max_climbed_job_id
  end


  def test_test_tube_is_initialized_but_configurable
    assert_equal StalkClimber::Connection::DEFAULT_TUBE, @connection.test_tube
    tube_name = 'test_tube'
    @connection.test_tube = tube_name
    assert_equal tube_name, @connection.test_tube
  end


  def test_with_job_yields_nil_or_the_requested_job_if_it_exists
    assert_nothing_raised do
      @connection.with_job(@connection.max_job_id) do |job|
        assert_equal nil, job
      end
    end

    probe = seed_jobs(1).first
    @connection.with_job(probe.id) do |job|
      assert_equal probe.id, job.id
    end
    probe.delete
  end


  def test_with_job_bang_does_not_execute_block_and_raises_error_if_job_does_not_exist
    block = lambda {}
    block.expects(:call).never
    assert_raise Beaneater::NotFoundError do
      Object.any_instance.expects(:yield).never
      @connection.with_job!(@connection.max_job_id, &block)
    end
  end

end
