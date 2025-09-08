module Neighbor
  module Redis
    class SvsVamanaIndex < Index
      def initialize(*args, **options)
        super(*args, **options)
        @algorithm = "SVS-VAMANA"
      end

      private

      def create_params
        params = {}
        params
      end
    end
  end
end
