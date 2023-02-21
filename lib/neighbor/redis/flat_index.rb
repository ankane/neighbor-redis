module Neighbor
  module Redis
    class FlatIndex < Index
      def initialize(*args, initial_cap: nil, block_size: nil, **options)
        super(*args, **options)
        @algorithm = "FLAT"
        @initial_cap = initial_cap
        @block_size = block_size
      end

      private

      def create_params
        params = {}
        params["INITIAL_CAP"] = @initial_cap if @initial_cap
        params["BLOCK_SIZE"] = @block_size if @block_size
        params
      end
    end
  end
end
