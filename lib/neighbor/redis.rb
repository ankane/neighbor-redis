# dependencies
require "redis-client"

# modules
require_relative "redis/index"
require_relative "redis/flat_index"
require_relative "redis/hnsw_index"
require_relative "redis/vector_set"
require_relative "redis/version"

module Neighbor
  module Redis
    class Error < StandardError; end

    class << self
      attr_accessor :client
    end
  end
end
