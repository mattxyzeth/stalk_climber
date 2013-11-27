require 'test_helper'

class ClimberEnumerableTest < StalkClimber::TestCase

  # Bulk of testing left to derived classes

  context '::new' do

    should 'return a new class with the specified enumerator method' do
      klass = StalkClimber::ClimberEnumerable.new(:foo)
      assert klass.included_modules.include?(StalkClimber::ClimberEnumerable), 'Expected ClimberEnumerable based class to include ClimberEnumerable'
      assert(klass.respond_to?(:enumerator_method), 'Expected ClimberEnumerable based class to implement :enumerator_method')
      assert_equal :foo, klass.enumerator_method
    end

  end

end
