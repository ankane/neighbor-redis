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

      def server_type
        unless defined?(@server_type)
          info = client.call("INFO")
          @server_type =
            if info.include?("valkey_version")
              :valkey
            elsif info.include?("dragonfly_version")
              :dragonfly
            else
              :redis
            end
        end
        @server_type
      end
    end
  end
end
