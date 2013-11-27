require 'test_helper'

class BreakableEnumeratorTest < StalkClimber::TestCase

  should 'allow for breaking enumeration and returning value' do
    enum = DummyBreakableEnumerator.new
    begin
      count = 0
      return_value = enum.each do |int|
        break :break! if 2 == count += 1
        assert(false, "BreakableEnumerator did not break when expected") if count >= 3
      end
    rescue => e
      assert(false, "Breaking from BreakableEnumerator raised #{e.inspect}")
    end
    assert_equal(:break!, return_value)
  end

end

class DummyBreakableEnumerator

  include StalkClimber::BreakableEnumerator

  def each
    breakable_enumerator(to_enum, &Proc.new)
  end


  def to_enum
    return Enumerator.new do |yielder|
      (1..10).each do |i|
        yielder << i
      end
    end
  end

end
