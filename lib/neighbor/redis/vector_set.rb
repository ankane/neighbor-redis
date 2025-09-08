module Neighbor
  module Redis
    class VectorSet
      NO_DEFAULT = Object.new

      def initialize(name, m: nil, ef_construction: nil, ef_runtime: nil, epsilon: nil, id_type: "string")
        name = name.to_str
        if name.include?(":")
          raise ArgumentError, "Invalid name"
        end

        @name = name
        @m = m&.to_i
        @ef_construction = ef_construction&.to_i
        @ef_runtime = ef_runtime&.to_i
        @epsilon = epsilon&.to_f

        case id_type
        when "string", "integer"
          @int_ids = id_type == "integer"
        else
          raise ArgumentError, "Invalid id_type"
        end
      end

      def exists?
        !run_command("VINFO", key).nil?
      end

      def info
        hash_result(run_command("VINFO", key))&.transform_keys { |k| k.gsub("-", "_").to_sym }
      end

      def count
        run_command("VCARD", key)
      end

      def add(id, vector, attributes: nil)
        id = item_id(id)

        args = []
        args.push("SETATTR", JSON.generate(attributes)) if attributes
        args.push("M", @m) if @m
        args.push("EF", @ef_construction) if @ef_construction
        bool_result(run_command("VADD", key, "FP32", to_binary(vector), id, "NOQUANT", *args))
      end

      def add_all(ids, vectors)
        ids = ids.to_a.map { |v| item_id(v) }
        vectors = vectors.to_a

        raise ArgumentError, "different sizes" if ids.size != vectors.size

        result =
          redis.pipelined do |pipeline|
            ids.zip(vectors) do |id, vector|
              pipeline.call("VADD", key, "FP32", to_binary(vector), id, "NOQUANT")
            end
          end
        result.map { |v| bool_result(v) }
      end

      def member?(id)
        id = item_id(id)

        bool_result(run_command("VISMEMBER", key, id))
      end
      alias_method :include?, :member?

      def remove(id)
        id = item_id(id)

        bool_result(run_command("VREM", key, id))
      end

      def remove_all(ids)
        ids.map { |id| remove(id) }
      end

      def find(id)
        id = item_id(id)

        run_command("VEMB", key, id)&.map(&:to_f)
      end

      def attributes(id)
        id = item_id(id)

        a = run_command("VGETATTR", key, id)
        a ? JSON.parse(a) : nil
      end

      def update_attributes(id, attributes)
        id = item_id(id)

        bool_result(run_command("VSETATTR", key, id, JSON.generate(attributes)))
      end

      def remove_attributes(id)
        id = item_id(id)

        bool_result(run_command("VSETATTR", key, id, ""))
      end

      def nearest(id, count: 5, with_attributes: false, ef: nil, exact: false)
        id = item_id(id)
        count = count.to_i

        result =
          nearest_command(["ELE", id], count: count + 1, with_attributes:, ef:, exact:).filter_map do |k, v|
            if k != id
              nearest_result(k, v, with_attributes:)
            end
          end
        result.first(count)
      end

      def search(vector, count: 5, with_attributes: false, ef: nil, exact: false)
        count = count.to_i

        nearest_command(["FP32", to_binary(vector)], count:, with_attributes:, ef:, exact:).map do |k, v|
          nearest_result(k, v, with_attributes:)
        end
      end

      def links(id)
        id = item_id(id)

        run_command("VLINKS", key, id, "WITHSCORES")&.map do |links|
          hash_result(links).map do |k, v|
            {id: cast_id(k), score: v.to_f}
          end
        end
      end

      def sample(n = NO_DEFAULT)
        count = n == NO_DEFAULT ? 1 : n.to_i

        result = run_command("VRANDMEMBER", key, count)
        n == NO_DEFAULT ? result.first : result
      end

      def drop
        bool_result(run_command("DEL", key))
      end

      private

      def key
        "neighbor:vs:#{@name}"
      end

      def item_id(id)
        @int_ids ? Integer(id).to_s : id.to_s
      end

      def cast_id(id)
        @int_ids ? Integer(id) : id
      end

      def to_binary(vector)
        vector.pack("e*")
      end

      def nearest_command(args, count:, with_attributes:, ef:, exact:)
        args << "WITHATTRIBS" if with_attributes

        ef = @ef_runtime if ef.nil?
        args.push("EF", ef) if ef

        args.push("EPSILON", @epsilon) if @epsilon

        args << "TRUTH" if exact

        result = run_command("VSIM", key, *args, "WITHSCORES", "COUNT", count)
        if result.is_a?(Array)
          if with_attributes
            result.each_slice(3).to_h { |v| [v[0], v[1..]] }
          else
            hash_result(result)
          end
        else
          result
        end
      end

      def nearest_result(k, v, with_attributes: false)
        v, a = v if with_attributes
        value = {id: cast_id(k), score: v.to_f}
        value.merge!(attributes: a ? JSON.parse(a) : {}) if with_attributes
        value
      end

      def hash_result(result)
        result.is_a?(Array) ? result.each_slice(2).to_h : result
      end

      def bool_result(result)
        result == true || result == 1
      end

      def run_command(*args)
        if args.any? { |v| !(v.is_a?(String) || v.is_a?(Numeric)) }
          raise TypeError, "Unexpected argument type"
        end
        redis.call(*args)
      end

      def redis
        Redis.client
      end
    end
  end
end
