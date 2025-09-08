module Neighbor
  module Redis
    class SvsVamanaIndex < Index
      def initialize(
        *args,
        compression: nil,
        construction_window_size: nil,
        graph_max_degree: nil,
        search_window_size: nil,
        epsilon: nil,
        training_threshold: nil,
        reduce: nil,
        **options
      )
        super(*args, **options)
        @algorithm = "SVS-VAMANA"
        @compression = compression
        @construction_window_size = construction_window_size
        @graph_max_degree = graph_max_degree
        @search_window_size = search_window_size
        @epsilon = epsilon
        @training_threshold = training_threshold
        @reduce = reduce
      end

      private

      def create_params
        params = {}
        params["COMPRESSION"] = @compression if @compression
        params["CONSTRUCTION_WINDOW_SIZE"] = @construction_window_size if @construction_window_size
        params["GRAPH_MAX_DEGREE"] = @graph_max_degree if @graph_max_degree
        params["SEARCH_WINDOW_SIZE"] = @search_window_size if @search_window_size
        params["EPSILON"] = @epsilon if @epsilon
        params["TRAINING_THRESHOLD"] = @training_threshold if @training_threshold
        params["REDUCE"] = @reduce if @reduce
        params
      end
    end
  end
end
