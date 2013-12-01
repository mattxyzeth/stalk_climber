require 'test_helper'

class TubeTest < StalkClimber::TestCase

  setup do
    @climber = StalkClimber::Climber.new(BEANSTALK_ADDRESSES)
    @tube_name = SecureRandom.uuid
    @climber.connection_pool.transmit_to_all("watch #{@tube_name}")
    @tube = StalkClimber::Tube.new(@climber.connection_pool, @tube_name)
  end


  context 'stats methods' do

    should 'should delegate stats method to stats' do
      stats_methods = StalkClimber::Tube::STATS_METHODS.values.reject do |method|
        method == 'pause'
      end
      stats_methods << 'pause_time'
      stats = @tube.stats
      # Take 1 of stats_method.length to account for name superclass method
      @tube.expects(:stats).times(stats_methods.length - 1).returns(stats)
      stats_methods.each {|stats_method| @tube.public_send(stats_method)}
    end

  end


  context 'exists?' do

    should 'return false if the tube no longer exists' do
      assert @tube.exists?
      @climber.connection_pool.transmit_to_all("ignore #{@tube_name}")
      refute @tube.exists?
    end


    should 'return true if the tube exists' do
      @climber.connection_pool.transmit_to_all("ignore #{@tube_name}")
      refute @tube.exists?
      @climber.connection_pool.transmit_to_all("watch #{@tube_name}")
      assert @tube.exists?
    end

  end


  context '#to_h' do

    should 'return the expected hash' do
      stats_body = {
        'cmd-delete' => 100,
        'cmd-pause-tube' => 101,
        'current-jobs-buried' => 102,
        'current-jobs-delayed' => 103,
        'current-jobs-ready' => 104,
        'current-jobs-reserved' => 105,
        'current-jobs-urgent' => 106,
        'current-using' => 107,
        'current-waiting' => 108,
        'current-watching' => 109,
        'name' => @tube_name,
        'pause-time-left' => 110,
        'pause' => 111,
        'total-jobs' => 112,
      }
      stats_response = {
        :body => stats_body,
        :connection => @connection,
        :id => 149,
        :status => 'OK',
      }
      @tube.expects(:transmit_to_all).once.returns(stats_response)
      expected_hash = Hash[stats_body.map { |k, v| [k, v] }]
      assert_equal expected_hash, @tube.to_h
    end

  end


  context '#to_s' do

    should 'return expected string representation of tube' do
      assert_equal "#<StalkClimber::Tube name=#{@tube_name}>", @tube.to_s
    end

  end

end
