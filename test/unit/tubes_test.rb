require 'test_helper'

class TubesTest < StalkClimber::TestCase

  setup do
    @climber = StalkClimber::Climber.new(BEANSTALK_ADDRESSES)
    @tubes = @climber.tubes
  end


  context 'compositional components' do

    should 'be some kind of Enumerable' do
      assert_kind_of Enumerable, @tubes
    end


    should 'be some kind of Beaneater::Tubes' do
      assert_kind_of Beaneater::Tubes, @tubes
    end

  end


  context '#each' do

    should 'delegate to Beaneater::Tubes#All' do
      @tubes.expects(:all).once.returns(stub(:each => []))
      @tubes.each
    end


    should 'iterate over all tubes in the pool yielding StalkClimber::Tube' do
      @test_tubes = []
      @climber.connection_pool.connections.each do |connection|
        2.times do
          @test_tubes << tube_name = SecureRandom.uuid
          connection.transmit("watch #{tube_name}")
        end
      end

      assert_equal @climber.connection_pool.connections.length * 2, @test_tubes.length
      @tubes.each do |tube|
        assert_kind_of StalkClimber::Tube, tube
        @test_tubes.delete(tube.name)
      end
      assert_equal [], @test_tubes
    end

  end

end
