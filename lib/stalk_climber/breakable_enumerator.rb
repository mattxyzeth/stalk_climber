module StalkClimber

  module BreakableEnumerator

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
