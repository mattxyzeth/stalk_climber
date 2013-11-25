require 'test_helper'

class ConnectionTest < Test::Unit::TestCase

  def setup
    @connection = StalkClimber::Connection.new('localhost:11300')
  end


  def test_cached_jobs_creates_and_returns_hash_instance_variable
    refute @connection.instance_variable_get(:@cached_jobs)
    assert_equal({}, @connection.cached_jobs)
    assert_equal({}, @connection.instance_variable_get(:@cached_jobs))
  end


  def test_cached_jobs_is_reset_if_max_job_id_lower_than_max_climbed_job_id
    seeds = seed_jobs
    @connection.each_job {}
    assert_not_equal({}, @connection.cached_jobs)
    assert_not_equal(Float::INFINITY, @connection.min_climbed_job_id)
    assert_not_equal(0, @connection.max_climbed_job_id)

    @connection.send(:update_climbed_job_ids_from_max_id, 1)
    assert_equal({}, @connection.cached_jobs)
    assert_equal(Float::INFINITY, @connection.min_climbed_job_id)
    assert_equal(0, @connection.max_climbed_job_id)
    seeds.map(&:delete)
  end


  def test_job_enumerator_is_some_kind_of_enumerable
    assert @connection.job_enumerator.kind_of?(Enumerable)
  end


  def test_deleted_jobs_should_not_be_enumerated
    seeds = seed_jobs
    seeds.map(&:delete)
    seed_ids = seeds.map(&:id)

    deleted = @connection.each_job.detect do |job|
      seed_ids.include?(job.id)
    end

    assert_nil deleted, "Deleted job found in enumeration: #{deleted}"
  end


  def test_each_job_caches_jobs_for_later_use
    seeds = seed_jobs

    jobs = {}
    @connection.each_job do |job|
      jobs[job.id] = job
    end

    @connection.expects(:with_job).never
    @connection.each_job do |job|
      assert_equal jobs[job.id], job
    end

    seeds.map(&:delete)
  end


  def test_each_job_deletes_cached_jobs_that_no_longer_exist
    seeds = seed_jobs

    jobs = {}
    @connection.each_job do |job|
      jobs[job.id] = job
    end

    deleted_job = jobs[jobs.keys[2]]
    deleted_job.delete

    @connection.expects(:with_job).never
    @connection.each_job do |job|
      assert_equal jobs.delete(job.id), job
    end

    assert_equal 1, jobs.length
    assert_equal deleted_job.id, jobs.values.first.id

    seeds.map(&:delete)
  end


  def test_each_job_hits_jobs_below_climbed_range_that_have_not_been_hit
    seeds = seed_jobs(10)

    count = 0
    @connection.each_job do |job|
      count += 1
      break if count == 5
    end

    initial_min_climbed_job_id = @connection.min_climbed_job_id

    all_jobs = {}
    @connection.each_job do |job|
      all_jobs[job.id] = job
    end

    seeds.each do |job|
      assert_equal all_jobs[job.id].body, job.body
      assert_equal all_jobs[job.id].id, job.id
      job.delete
    end

    assert @connection.min_climbed_job_id < initial_min_climbed_job_id
  end


  def test_each_job_only_hits_each_job_once
    seeds = seed_jobs

    jobs = {}
    @connection.each_job do |job|
      refute jobs[job.id]
      jobs[job.id] = job
    end

    seeds.map(&:delete)
  end


  def test_each_job_calls_job_enumerator
    @connection.expects(:job_enumerator).returns(Enumerator.new { |yielder| yielder << 1 })
    @connection.each_job {}
  end


  def test_fetch_job_returns_nil_or_the_requested_job_if_it_exists
    assert_nothing_raised do
      job = @connection.fetch_job(@connection.max_job_id)
      assert_equal nil, job
    end

    probe = seed_jobs(1).first
    assert_equal probe.id, @connection.fetch_job(probe.id).id
    probe.delete
  end


  def test_fetch_job_bang_returns_requested_job_or_raises_an_error_if_job_does_not_exist
    assert_raise Beaneater::NotFoundError do
      @connection.fetch_job!(@connection.max_job_id)
    end

    probe = seed_jobs(1).first
    assert_equal probe.id, @connection.fetch_job!(probe.id).id
    probe.delete
  end


  def test_fetch_jobs_returns_each_requested_job_or_nil
    assert_nothing_raised do
      jobs = @connection.fetch_jobs(@connection.max_job_id, @connection.max_job_id)
      assert_equal [nil, nil], jobs
    end

    probes = seed_jobs(2)
    probe_ids = probes.map(&:id)
    assert_equal probe_ids, @connection.fetch_jobs(probe_ids).map(&:id)
    probes.map(&:delete)
  end


  def test_fetch_jobs_bang_returns_requested_jobs_or_raises_an_error_if_any_job_does_not_exist
    probes = seed_jobs(2)

    probe_ids = probes.map(&:id)
    assert_equal probe_ids, @connection.fetch_jobs!(probe_ids).map(&:id)

    assert_raise Beaneater::NotFoundError do
      @connection.fetch_jobs!(probe_ids.first, @connection.max_job_id, probe_ids.last)
    end

    probes.map(&:delete)
  end


  def test_max_job_id_returns_expected_max_job_id
    initial_max = @connection.max_job_id
    seed_jobs(3).map(&:delete)
    # 3 new jobs, +1 for the probe job
    assert_equal initial_max + 4, @connection.max_job_id
  end


  def test_max_job_id_should_increment_max_climbed_id_if_successor
    @connection.each_job {}
    max = @connection.max_climbed_job_id
    @connection.max_job_id
    assert_equal max + 1, @connection.max_climbed_job_id
  end


  def test_max_job_id_should_not_increment_max_climbed_id_unless_successor
    @connection.each_job {}
    max = @connection.max_climbed_job_id
    seed_jobs(1).map(&:delete)
    @connection.max_job_id
    assert_equal max, @connection.max_climbed_job_id
  end


  def test_each_job_sets_min_and_max_climbed_job_ids_appropriately
    assert_equal Float::INFINITY, @connection.min_climbed_job_id
    assert_equal 0, @connection.max_climbed_job_id

    max_id = @connection.max_job_id
    @connection.expects(:max_job_id).once.returns(max_id)
    @connection.each_job {}

    assert_equal 1, @connection.min_climbed_job_id
    assert_equal max_id, @connection.max_climbed_job_id
  end


  def test_initialize_with_a_test_tube
    StalkClimber::Connection.any_instance.expects(:use_test_tube)
    connection = StalkClimber::Connection.new('localhost:11300', 'test_tube')
    assert_equal 'test_tube', connection.test_tube
  end


  def test_test_tube_is_initialized_but_configurable
    assert_equal StalkClimber::Connection::DEFAULT_TUBE, @connection.test_tube
    tube_name = 'test_tube'
    @connection.test_tube = tube_name
    assert_equal tube_name, @connection.test_tube
  end


  def test_setting_test_tube_uses_test_tube
    @connection.expects(:transmit).with('use test_tube')
    @connection.expects(:transmit).with('watch test_tube')
    @connection.expects(:transmit).with('ignore default')
    @connection.test_tube = 'test_tube'
    assert_equal 'test_tube', @connection.test_tube
  end


  def test_to_enum_returns_an_enumerator
    assert_kind_of Enumerator, @connection.to_enum
  end

end
