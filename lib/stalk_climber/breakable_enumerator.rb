module StalkClimber

  module BreakableEnumerator

    # :call-seq:
    #   breakable_enumerator(enumerator, &block) => Object
    #
    # Allows for enumerating an enumerator in such a way that it supports
    # breaking from the enumeration without error. The method returns the value
    # passed to break in the given +block+ or nil.
    #
    #   class DummyEnumerator
    #     include StalkClimber::BreakableEnumerator
    #
    #     def each
    #       breakable_enumerator((1..10).each, Proc.new)
    #     end
    #   end
    #
    #   dummy = DummyEnumerator.new
    #   dummy.each do |i|
    #     break i
    #   end
    #     #=> 1
    def breakable_enumerator(enumerator, &block)
      return enumerator if block.nil?
      loop do
        begin
          block.call(enumerator.next)
        rescue StopIteration => e
          return e.result if e.respond_to?(:result)
          return e.exit_value if e.respond_to?(:exit_value)
          return nil
        end
      end
    end

  end

end
