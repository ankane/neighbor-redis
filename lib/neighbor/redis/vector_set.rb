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

      def add(id, vector, attributes: {})
        id = item_id(id)
        check_dimensions(vector)

        args = []
        args.concat(["SETATTR", JSON.generate(attributes)]) if attributes.any?
        redis.call("VADD", key, "FP32", to_binary(vector), id, "NOQUANT", *args)
      end

      def remove(id)
        id = item_id(id)

        redis.call("VREM", key, id)
      end

      def find(id)
        id = item_id(id)

        redis.call("VEMB", key, id)
      end

      def nearest_by_id(id, count: 5, with_attributes: false)
        id = item_id(id)
        count = count.to_i

        args = []
        args << "WITHATTRIBS" if with_attributes
        result =
          redis.call("VSIM", key, "ELE", id, "WITHSCORES", "COUNT", count + 1, *args).filter_map do |k, v|
            if k != id
              v, a = v if with_attributes
              value = {id: k, score: v}
              value.merge!(attributes: a ? JSON.parse(a) : {}) if with_attributes
              value
            end
          end
        result.first(count)
      end

      def nearest_by_vector(vector, count: 5)
        check_dimensions(vector)
        count = count.to_i

        redis.call("VSIM", key, "FP32", to_binary(vector), "WITHSCORES", "COUNT", count).map do |k, v|
          {id: k, score: v}
        end
      end

      def member?(id)
        id = item_id(id)

        redis.call("VISMEMBER", key, id)
      end
      alias_method :include?, :member?

      def count
        redis.call("VCARD", key)
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
