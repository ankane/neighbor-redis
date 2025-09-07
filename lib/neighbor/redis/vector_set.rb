module Neighbor
  module Redis
    class VectorSet
      def initialize(name, dimensions:)
        name = name.to_str
        if name.include?(":")
          raise ArgumentError, "Invalid name"
        end

        @name = name
        @dimensions = dimensions.to_i
      end

      def exists?
        !redis.call("VINFO", key).nil?
      end

      def info
        redis.call("VINFO", key)&.transform_keys { |k| k.gsub("-", "_").to_sym }
      end

      def add(id, vector, attributes: nil)
        id = item_id(id)
        check_dimensions(vector)

        args = []
        args.concat(["SETATTR", JSON.generate(attributes)]) if attributes
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

      def find_attributes(id)
        id = item_id(id)

        a = redis.call("VGETATTR", key, id)
        a ? JSON.parse(a) : nil
      end

      def update_attributes(id, attributes)
        id = item_id(id)

        redis.call("VSETATTR", key, id, JSON.generate(attributes))
      end

      def remove_attributes(id)
        id = item_id(id)

        redis.call("VSETATTR", key, id, "")
      end

      def nearest_by_id(id, count: 5, with_attributes: false)
        id = item_id(id)
        count = count.to_i

        result =
          nearest(["ELE", id], count: count + 1, with_attributes:).filter_map do |k, v|
            if k != id
              nearest_result(k, v, with_attributes:)
            end
          end
        result.first(count)
      end

      def nearest_by_vector(vector, count: 5, with_attributes: false)
        check_dimensions(vector)
        count = count.to_i

        nearest(["FP32", to_binary(vector)], count:, with_attributes:).map do |k, v|
          nearest_result(k, v, with_attributes:)
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

      def nearest(args, count:, with_attributes:)
        args << "WITHATTRIBS" if with_attributes
        redis.call("VSIM", key, args, "WITHSCORES", "COUNT", count)
      end

      def nearest_result(k, v, with_attributes:)
        v, a = v if with_attributes
        value = {id: k, score: v}
        value.merge!(attributes: a ? JSON.parse(a) : {}) if with_attributes
        value
      end

      def redis
        Redis.client
      end
    end
  end
end
