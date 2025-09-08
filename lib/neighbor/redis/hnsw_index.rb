module Neighbor
  module Redis
    class HnswIndex < Index
      def initialize(
        *args,
        initial_cap: nil,
        m: nil,
        ef_construction: nil,
        ef_runtime: nil,
        epsilon: nil,
        **options
      )
        super(*args, **options)
        @algorithm = "HNSW"
        @initial_cap = initial_cap
        @m = m
        @ef_construction = ef_construction
        @ef_runtime = ef_runtime
        @epsilon = epsilon
      end

      private

      def create_params
        params = {}
        params["INITIAL_CAP"] = @initial_cap if @initial_cap
        params["M"] = @m if @m
        params["EF_CONSTRUCTION"] = @ef_construction if @ef_construction
        params["EF_RUNTIME"] = @ef_runtime if @ef_runtime
        params["EPSILON"] = @epsilon if @epsilon
        params
      end
    end

    HNSWIndex = HnswIndex
  end
end
