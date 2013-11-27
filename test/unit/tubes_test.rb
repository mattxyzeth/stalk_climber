require 'test_helper'
require 'debugger'

class TubesTest < StalkClimber::TestCase

  context '#each' do

    setup do
      @climber = StalkClimber::Climber.new(BEANSTALK_ADDRESSES)

      @test_tubes = {}
    end

    should '' do
      debugger
      @climber.connection_pool.connections.first.tube_enumerator
    end

  end

end
