module Neighbor
  module Redis
    class VectorSet
      NO_DEFAULT = Object.new

      def initialize(name, dimensions:)
        name = name.to_str
        if name.include?(":")
          raise ArgumentError, "Invalid name"
        end

        @name = name
        @dimensions = dimensions.to_i
      end

      def exists?
        !run_command("VINFO", key).nil?
      end

      def info
        run_command("VINFO", key)&.transform_keys { |k| k.gsub("-", "_").to_sym }
      end

      def add(id, vector, attributes: nil)
        id = item_id(id)
        check_dimensions(vector)

        args = []
        args.concat(["SETATTR", JSON.generate(attributes)]) if attributes
        run_command("VADD", key, "FP32", to_binary(vector), id, "NOQUANT", *args)
      end

      def remove(id)
        id = item_id(id)

        run_command("VREM", key, id)
      end

      def find(id)
        id = item_id(id)

        run_command("VEMB", key, id)
      end

      def find_attributes(id)
        id = item_id(id)

        a = run_command("VGETATTR", key, id)
        a ? JSON.parse(a) : nil
      end

      def update_attributes(id, attributes)
        id = item_id(id)

        run_command("VSETATTR", key, id, JSON.generate(attributes))
      end

      def remove_attributes(id)
        id = item_id(id)

        run_command("VSETATTR", key, id, "")
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

        run_command("VISMEMBER", key, id)
      end
      alias_method :include?, :member?

      def count
        run_command("VCARD", key)
      end

      def sample(n = NO_DEFAULT)
        count = n == NO_DEFAULT ? 1 : n.to_i

        result = run_command("VRANDMEMBER", key, count)
        n == NO_DEFAULT ? result.first : result
      end

      def drop
        run_command("DEL", key)
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
        run_command("VSIM", key, *args, "WITHSCORES", "COUNT", count)
      end

      def nearest_result(k, v, with_attributes:)
        v, a = v if with_attributes
        value = {id: k, score: v}
        value.merge!(attributes: a ? JSON.parse(a) : {}) if with_attributes
        value
      end

      def run_command(*args)
        if args.any? { |v| !(v.is_a?(String) || v.is_a?(Integer)) }
          raise TypeError, "Unexpected argument type"
        end
        redis.call_v(args)
      end

      def redis
        Redis.client
      end
    end
  end
end
