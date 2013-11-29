module StalkClimber
  module LazyEnumerable
    include Enumerable

    # Convert Enumerable public methods to their lazy counterparts
    def self.make_lazy(*methods)
      methods.each do |method|
        define_method method do |*args, &block|
          lazy.public_send(method, *args, &block)
        end
      end
    end

    make_lazy(*(Enumerable.public_instance_methods - [:lazy]))
  end
end
