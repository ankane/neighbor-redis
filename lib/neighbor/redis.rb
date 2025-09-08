# dependencies
require "redis-client"

# stdlib
require "json"

# modules
require_relative "redis/index"
require_relative "redis/flat_index"
require_relative "redis/hnsw_index"
require_relative "redis/svs_vamana_index"
require_relative "redis/vector_set"
require_relative "redis/version"

module Neighbor
  module Redis
    class Error < StandardError; end

    class << self
      attr_accessor :client

      def valkey?
        unless defined?(@valkey)
          @valkey = client.call("INFO").include?("valkey_version")
        end
        @valkey
      end
    end
  end
end
