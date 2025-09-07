module Neighbor
  module Redis
    class VectorSet
      def initialize(name, dimensions:)
        if name.include?(":")
          raise ArgumentError, "Invalid name"
        end

        @name = name
        @dimensions = dimensions.to_i
      end

      def exists?
        !redis.call("VINFO", key).nil?
      end

      def add(id, vector)
        id = item_id(id)
        check_dimensions(vector)

        redis.call("VADD", key, "FP32", to_binary(vector), id, "NOQUANT")
      end

      def remove(id)
        id = item_id(id)

        redis.call("VREM", key, id)
      end

      def nearest_by_id(id, count: 5)
        id = item_id(id)
        count = count.to_i

        result =
          redis.call("VSIM", key, "ELE", id, "WITHSCORES", "COUNT", count + 1).filter_map do |k, v|
            if k != id
              {id: k, score: v}
            end
          end
        result.first(count)
      end

      def nearest_by_vector(vector, count: 5)
        check_dimensions(vector)
        count = count.to_i

        redis.call("VSIM", key, "FP32", to_binary(vector), "WITHSCORES", "COUNT", count).filter_map do |k, v|
          {id: k, score: v}
        end
      end

      def drop
        redis.call("DEL", key)
      end

      private

      def key
        "neighbor:vs:#{@name}"
      end

      def item_id(id)
        id.to_s
      end

      def to_binary(vector)
        vector.pack("e*")
      end

      def check_dimensions(vector)
        if vector.size != @dimensions
          raise ArgumentError, "dimension mismatch"
        end
      end

      def redis
        Redis.client
      end
    end
  end
end
