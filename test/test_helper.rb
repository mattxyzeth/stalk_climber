require 'coveralls'
Coveralls.wear!

lib = File.expand_path('../../lib', __FILE__)
$LOAD_PATH.unshift(lib) unless $LOAD_PATH.include?(lib)

require 'test/unit'
require 'mocha/setup'
require 'minitest/autorun'
require 'minitest/should'
require 'stalk_climber'

BEANSTALK_ADDRESS = ENV['BEANSTALK_ADDRESS'] || 'beanstalk://localhost'
BEANSTALK_ADDRESSES = ENV['BEANSTALK_ADDRESSES'] || BEANSTALK_ADDRESS

class StalkClimber::TestCase < MiniTest::Should::TestCase

  def seed_jobs(count = 5)
    count.times.map do
      StalkClimber::Job.new(@connection.transmit(StalkClimber::Connection::PROBE_TRANSMISSION))
    end
  end

end
