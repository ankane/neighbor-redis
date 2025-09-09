module Neighbor
  module Redis
    class VectorSet
      NO_DEFAULT = Object.new

      def initialize(
        name,
        m: nil,
        ef_construction: nil,
        ef_search: nil,
        epsilon: nil,
        quantization: nil,
        reduce: nil,
        id_type: "string"
      )
        name = name.to_str
        if name.include?(":")
          raise ArgumentError, "invalid name"
        end

        @name = name
        @m = m&.to_i
        @ef_construction = ef_construction&.to_i
        @ef_search = ef_search&.to_i
        @epsilon = epsilon&.to_f

        @quant_type =
          case quantization&.to_s
          when nil
            "NOQUANT"
          when "binary"
            "BIN"
          when "int8"
            "Q8"
          else
            raise ArgumentError, "invalid quantization"
          end

        case id_type.to_s
        when "string", "integer"
          @int_ids = id_type == "integer"
        else
          raise ArgumentError, "invalid id_type"
        end

        @reduce_args = []
        @reduce_args.push("REDUCE", reduce.to_i) if reduce

        @add_args = []
        @add_args.push("M", @m) if @m
        @add_args.push("EF", @ef_construction) if @ef_construction
      end

      def exists?
        !run_command("VINFO", key).nil?
      end

      def info
        hash_result(run_command("VINFO", key))&.transform_keys { |k| k.gsub("-", "_").to_sym }
      end

      def dimensions
        run_command("VDIM", key)
      rescue => e
        raise e unless e.message.include?("key does not exist")
        nil
      end

      def count
        run_command("VCARD", key)
      end

      def add(id, vector, metadata: nil)
        add_all([id], [vector], metadata: metadata ? [metadata] : nil)[0]
      end

      def add_all(ids, vectors, metadata: nil)
        ids = ids.to_a.map { |v| item_id(v) }
        vectors = vectors.to_a
        metadata = metadata.to_a if metadata

        raise ArgumentError, "different sizes" if ids.size != vectors.size

        # check first to avoid non-atomic update if different
        if vectors.size > 1
          dimensions = vectors.first.size
          unless vectors.all? { |v| v.size == dimensions }
            raise ArgumentError, "different dimensions"
          end
        end

        if metadata
          raise ArgumentError, "different sizes" if metadata.size != ids.size
        end

        result =
          client.pipelined do |pipeline|
            ids.zip(vectors).each_with_index do |(id, vector), i|
              attributes = metadata[i] if metadata
              attribute_args = []
              attribute_args.push("SETATTR", JSON.generate(attributes)) if attributes
              pipeline.call("VADD", key, *@reduce_args, "FP32", to_binary(vector), id, @quant_type, *attribute_args, *@add_args)
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

      def metadata(id)
        id = item_id(id)

        a = run_command("VGETATTR", key, id)
        a ? JSON.parse(a) : nil
      end

      def set_metadata(id, metadata)
        id = item_id(id)

        bool_result(run_command("VSETATTR", key, id, JSON.generate(metadata)))
      end

      def remove_metadata(id)
        id = item_id(id)

        bool_result(run_command("VSETATTR", key, id, ""))
      end

      def search(vector, count: 5, with_metadata: false, ef: nil, exact: false, _filter: nil)
        count = count.to_i

        search_command(["FP32", to_binary(vector)], count:, with_metadata:, ef:, exact:, _filter:).map do |k, v|
          search_result(k, v, with_metadata:)
        end
      end

      def search_id(id, count: 5, with_metadata: false, ef: nil, exact: false, _filter: nil)
        id = item_id(id)
        count = count.to_i

        result =
          search_command(["ELE", id], count: count + 1, with_metadata:, ef:, exact:, _filter:).filter_map do |k, v|
            if k != id.to_s
              search_result(k, v, with_metadata:)
            end
          end
        result.first(count)
      end
      alias_method :nearest, :search_id

      def links(id)
        id = item_id(id)

        run_command("VLINKS", key, id, "WITHSCORES")&.map do |links|
          hash_result(links).map do |k, v|
            search_result(k, v)
          end
        end
      end

      def sample(n = NO_DEFAULT)
        count = n == NO_DEFAULT ? 1 : n.to_i

        result = run_command("VRANDMEMBER", key, count).map { |v| item_id(v) }
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
        @int_ids ? Integer(id) : id.to_s
      end

      def to_binary(vector)
        vector.pack("e*")
      end

      def search_command(args, count:, with_metadata:, ef:, exact:, _filter:)
        ef = @ef_search if ef.nil?

        args << "WITHATTRIBS" if with_metadata
        args.push("EF", ef) if ef
        args.push("EPSILON", @epsilon) if @epsilon
        args.push("FILTER", _filter) if _filter
        args << "TRUTH" if exact

        result = run_command("VSIM", key, *args, "WITHSCORES", "COUNT", count)
        if result.is_a?(Array)
          if with_metadata
            result.each_slice(3).to_h { |v| [v[0], v[1..]] }
          else
            hash_result(result)
          end
        else
          result
        end
      end

      def search_result(k, v, with_metadata: false)
        v, a = v if with_metadata
        value = {id: item_id(k), distance: 2 * (1 - v.to_f)}
        value.merge!(metadata: a ? JSON.parse(a) : {}) if with_metadata
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
          raise TypeError, "unexpected argument type"
        end
        client.call(*args)
      end

      def client
        Redis.client
      end
    end
  end
end
