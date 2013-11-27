require 'test_helper'

class ConnectionTest < StalkClimber::TestCase

  setup do
    @connection = StalkClimber::Connection.new('localhost:11300')
  end


  context '#cached_jobs' do

    should 'create and return a hash instance variable' do
      refute @connection.instance_variable_get(:@cached_jobs)
      assert_equal({}, @connection.cached_jobs)
      assert_equal({}, @connection.instance_variable_get(:@cached_jobs))
    end


    should 'reset if max job id lower than max climbed job id' do
      seeds = seed_jobs
      @connection.each_job {}
      refute_equal({}, @connection.cached_jobs)
      refute_equal(Float::INFINITY, @connection.min_climbed_job_id)
      refute_equal(0, @connection.max_climbed_job_id)

      @connection.send(:update_climbed_job_ids_from_max_id, 1)
      assert_equal({}, @connection.cached_jobs)
      assert_equal(Float::INFINITY, @connection.min_climbed_job_id)
      assert_equal(0, @connection.max_climbed_job_id)
      seeds.map(&:delete)
    end

  end


  context '#job_enumerator' do

    should 'be some kind of enumerable' do
      assert @connection.job_enumerator.kind_of?(Enumerable)
    end

  end


  context '#each_job' do

    should 'not enumerate deleted jobs' do
      seeds = seed_jobs
      seeds.map(&:delete)
      seed_ids = seeds.map(&:id)

      deleted = @connection.each_job.detect do |job|
        seed_ids.include?(job.id)
      end

      assert_nil deleted, "Deleted job found in enumeration: #{deleted}"
    end


    should 'cache jobs for later use' do
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


    should 'delete cached jobs that no longer exists' do
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


    should 'hit jobs below climbed range that have not been previously hit' do
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


    should 'hit each job only once' do
      seeds = seed_jobs

      jobs = {}
      @connection.each_job do |job|
        refute jobs[job.id]
        jobs[job.id] = job
      end

      seeds.map(&:delete)
    end


    should 'get enumerator via #job_enumerator' do
      @connection.expects(:job_enumerator).returns(Enumerator.new { |yielder| yielder << 1 })
      @connection.each_job {}
    end


    should 'set min and max climbed job ids appropriately' do
      assert_equal Float::INFINITY, @connection.min_climbed_job_id
      assert_equal 0, @connection.max_climbed_job_id

      max_id = @connection.max_job_id
      @connection.expects(:max_job_id).once.returns(max_id)
      @connection.each_job {}

      assert_equal 1, @connection.min_climbed_job_id
      assert_equal max_id, @connection.max_climbed_job_id
    end


    should 'allow for breaking enumeration' do
      begin
        count = 0
        @connection.each_job do |job|
          break if 2 == count += 1
          assert(false, "Connection#each_job did not break when expected") if count >= 3
        end
      rescue => e
        assert(false, "Breaking from Connection#each_job raised #{e.inspect}")
      end
    end

  end

  context '#fetch_job' do

    should 'return nil or the requested job if it exists' do
      job = @connection.fetch_job(@connection.max_job_id)
      assert_equal nil, job

      probe = seed_jobs(1).first
      assert_equal probe.id, @connection.fetch_job(probe.id).id
      probe.delete
    end

  end


  context '#fetch_job!' do

    should 'return requested job or raise an error if the job does not exists' do
      assert_raises Beaneater::NotFoundError do
        @connection.fetch_job!(@connection.max_job_id)
      end

      probe = seed_jobs(1).first
      assert_equal probe.id, @connection.fetch_job!(probe.id).id
      probe.delete
    end

  end


  context '#fetch_jobs' do

    should 'return each requested job or nil' do
      jobs = @connection.fetch_jobs(@connection.max_job_id, @connection.max_job_id)
      assert_equal [nil, nil], jobs

      probes = seed_jobs(2)
      probe_ids = probes.map(&:id)
      assert_equal probe_ids, @connection.fetch_jobs(probe_ids).map(&:id)
      probes.map(&:delete)
    end

  end


  context '#fetch_jobs!' do

    should 'return the requested jobs or raise an error if any job does not exist' do
      probes = seed_jobs(2)

      probe_ids = probes.map(&:id)
      assert_equal probe_ids, @connection.fetch_jobs!(probe_ids).map(&:id)

      assert_raises Beaneater::NotFoundError do
        @connection.fetch_jobs!(probe_ids.first, @connection.max_job_id, probe_ids.last)
      end

      probes.map(&:delete)
    end

  end


  context '#max_job_id' do

    should 'return expected max job id' do
      initial_max = @connection.max_job_id
      seed_jobs(3).map(&:delete)
      # 3 new jobs, +1 for the probe job
      assert_equal initial_max + 4, @connection.max_job_id
    end


    should 'increment max climbed id if successor visited' do
      @connection.each_job {}
      max = @connection.max_climbed_job_id
      @connection.max_job_id
      assert_equal max + 1, @connection.max_climbed_job_id
    end


    should 'not increment max_climbed_id unless visiting successor' do
      @connection.each_job {}
      max = @connection.max_climbed_job_id
      seed_jobs(1).map(&:delete)
      @connection.max_job_id
      assert_equal max, @connection.max_climbed_job_id
    end

  end


  context '#initialize' do

    should 'initialize with a test tube' do
      StalkClimber::Connection.any_instance.expects(:use_test_tube)
      connection = StalkClimber::Connection.new('localhost:11300', 'test_tube')
      assert_equal 'test_tube', connection.test_tube
    end

  end

  context '#test_tube=' do

    should 'initialize with test tube but allow configuration' do
      assert_equal StalkClimber::Connection::DEFAULT_TUBE, @connection.test_tube
      tube_name = 'test_tube'
      @connection.test_tube = tube_name
      assert_equal tube_name, @connection.test_tube
    end


    should 'use the supplied test tube' do
      @connection.expects(:transmit).with('use test_tube')
      @connection.expects(:transmit).with('watch test_tube')
      @connection.expects(:transmit).with('ignore default')
      @connection.test_tube = 'test_tube'
      assert_equal 'test_tube', @connection.test_tube
    end

  end


  context '#to_enum' do

    should 'return an enumerator' do
      assert_kind_of Enumerator, @connection.to_enum
    end

  end

end
