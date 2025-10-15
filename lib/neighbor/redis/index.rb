module Neighbor
  module Redis
    class Index
      def initialize(name, dimensions:, distance:, type: "float32", redis_type: "hash", id_type: "string")
        @index_name = index_name(name)
        @global_prefix = "neighbor:items:"
        @prefix = "#{@global_prefix}#{name}:"

        @dimensions = dimensions.to_i

        unless distance.nil?
          @distance_metric =
            case distance.to_s
            when "l2"
              "L2"
            when "inner_product"
              "IP"
            when "cosine"
              if Redis.server_type == :dragonfly
                # uses inner product instead of cosine distance?
                raise ArgumentError, "unsupported distance"
              else
                "COSINE"
              end
            else
              raise ArgumentError, "invalid distance"
            end
        end

        @float64 =
          case type.to_s
          when "float32"
            false
          when "float64"
            true
          else
            raise ArgumentError, "invalid type"
          end

        @json =
          case redis_type.to_s
          when "hash"
            false
          when "json"
            require "json"
            true
          else
            raise ArgumentError, "invalid redis_type"
          end

        @int_ids =
          case id_type.to_s
          when "string"
            false
          when "integer"
            true
          else
            raise ArgumentError, "invalid id_type"
          end
      end

      def self.create(*args, _schema: nil, **options)
        index = new(*args, **options)
        index.create(_schema:)
        index
      end

      def create(_schema: nil)
        params = {
          "TYPE" => @float64 ? "FLOAT64" : "FLOAT32",
          "DIM" => @dimensions,
          "DISTANCE_METRIC" => @distance_metric
        }.merge(create_params)

        command = ["FT.CREATE", @index_name]
        command.push("ON", "JSON") if @json
        command.push("PREFIX", "1", @prefix, "SCHEMA")
        command.push("$.v", "AS") if @json
        command.push("v", "VECTOR", @algorithm, params.size * 2)
        params.each do |k, v|
          command.push(k, v)
        end

        (_schema || {}).each do |k, v|
          k = k.to_s
          # TODO improve
          if k == "v" || !k.match?(/\A\w+\z/)
            raise ArgumentError, "invalid schema"
          end
          command.push("$.#{k}", "AS") if @json
          command.push(k, v.to_s)
          # TODO figure out how to handle separator for hashes
          # command.push("SEPARATOR", "") if !@json
        end

        run_command(*command)
        nil
      rescue => e
        raise Error, "RediSearch not installed" if e.message.include?("ERR unknown command 'FT.")
        raise e
      end

      def exists?
        run_command("FT.INFO", @index_name)
        true
      rescue ArgumentError
        # fix for invalid value for Float(): "-nan"
        true
      rescue => e
        message = e.message.downcase
        raise e unless message.include?("unknown index name") || message.include?("no such index") || message.include?("not found")
        false
      end

      def info
        info = run_command("FT.INFO", @index_name)
        if info.is_a?(Hash)
          info
        else
          # for RESP2
          info = hash_result(info)
          ["index_definition", "gc_stats" ,"cursor_stats", "dialect_stats", "Index Errors"].each do |k|
            info[k] = hash_result(info[k]) if info[k]
          end
          ["attributes", "field statistics"].each do |k|
            info[k]&.map! { |v| hash_result(v) }
          end
          info["field statistics"]&.each do |v|
            v["Index Errors"] = hash_result(v["Index Errors"]) if v["Index Errors"]
          end
          info
        end
      end

      def count
        info.fetch("num_docs").to_i
      end

      def add(id, vector, metadata: nil)
        add_all([id], [vector], metadata: metadata ? [metadata] : nil)[0]
      end

      def add_all(ids, vectors, metadata: nil)
        # perform checks first to reduce chance of non-atomic updates
        ids = ids.to_a.map { |v| item_id(v) }
        vectors = vectors.to_a
        metadata = metadata.to_a if metadata

        raise ArgumentError, "different sizes" if ids.size != vectors.size

        vectors.each { |e| check_dimensions(e) }

        if metadata
          raise ArgumentError, "different sizes" if metadata.size != ids.size

          metadata = metadata.map { |v| v&.transform_keys(&:to_s) }
          if metadata.any? { |v| v&.key?("v") }
            # TODO improve
            raise ArgumentError, "invalid metadata"
          end
        end

        result =
          client.pipelined do |pipeline|
            ids.zip(vectors).each_with_index do |(id, vector), i|
              attributes = metadata && metadata[i] || {}
              if @json
                pipeline.call("JSON.SET", item_key(id), "$", JSON.generate(attributes.merge({"v" => vector})))
              else
                pipeline.call("HSET", item_key(id), attributes.merge({"v" => to_binary(vector)}))
              end
            end
          end
        result.map { |v| v.is_a?(String) ? v == "OK" : v > 0 }
      end

      def member?(id)
        key = item_key(id)

        run_command("EXISTS", key) == 1
      end
      alias_method :include?, :member?

      def remove(id)
        remove_all([id]) == 1
      end

      def remove_all(ids)
        keys = ids.to_a.map { |id| item_key(id) }

        run_command("DEL", *keys).to_i
      end

      def find(id)
        key = item_key(id)

        if @json
          s = run_command("JSON.GET", key, "$.v")
          JSON.parse(s)[0] if s
        else
          s = run_command("HGET", key, "v")
          from_binary(s) if s
        end
      end

      def find_in_batches(batch_size: 1000)
        cursor = 0
        prefix_length = nil
        begin
          cursor, keys = run_command("SCAN", cursor, "MATCH", "#{@prefix}*", "COUNT", batch_size)

          items =
            if @json
              keys.filter_map do |key|
                v = run_command("JSON.GET", key, "$")
                if v
                  prefix_length ||= find_prefix_length(key)
                  attributes = JSON.parse(v)[0]
                  {
                    id: item_id(key[prefix_length..-1]),
                    vector: attributes.delete("v"),
                    metadata: attributes
                  }
                end
              end
            else
              keys.filter_map do |key|
                v = run_command("HGETALL", key)
                if v
                  prefix_length ||= find_prefix_length(key)
                  {
                    id: item_id(key[prefix_length..-1]),
                    vector: from_binary(v.delete("v")),
                    metadata: v
                  }
                end
              end
            end

          # TODO always yield exact batch size
          yield items if items.any?
        end while cursor != "0"
      end

      def metadata(id)
        key = item_key(id)

        if @json
          v = run_command("JSON.GET", key)
          JSON.parse(v).except("v") if v
        else
          v = hash_result(run_command("HGETALL", key))
          v.except("v") if v.any?
        end
      end

      def set_metadata(id, metadata)
        key = item_key(id)

        # TODO DRY with add_all
        metadata = metadata.transform_keys(&:to_s)
        raise ArgumentError, "invalid metadata" if metadata.key?("v")

        if @json
          # TODO use WATCH
          keys = run_command("JSON.OBJKEYS", key)
          return false unless keys

          keys.each do |k|
            next if k == "v"

            # safe to modify in-place
            metadata[k] = nil unless metadata.key?(k)
          end

          run_command("JSON.MERGE", key, "$", JSON.generate(metadata)) == "OK"
        else
          # TODO use WATCH
          fields = run_command("HKEYS", key)
          return false if fields.empty?

          fields.delete("v")
          if fields.any?
            # TODO use MULTI
            run_command("HDEL", key, *fields)
          end

          if metadata.any?
            args = []
            metadata.each do |k, v|
              args.push(k, v)
            end
            run_command("HSET", key, *args) > 0
          else
            true
          end
        end
      end

      def remove_metadata(id)
        key = item_key(id)

        if @json
          # TODO use WATCH
          keys = run_command("JSON.OBJKEYS", key)
          return false unless keys

          keys.delete("v")
          if keys.any?
            # merge with null deletes key
            run_command("JSON.MERGE", key, "$", JSON.generate(keys.to_h { |k| [k, nil] })) == "OK"
          else
            true
          end
        else
          # TODO use WATCH
          fields = run_command("HKEYS", key)
          return false if fields.empty?

          fields.delete("v")
          if fields.any?
            run_command("HDEL", key, *fields) > 0
          else
            true
          end
        end
      end

      def search(vector, count: 5, with_metadata: false, _filter: nil)
        check_dimensions(vector)

        search_command(to_binary(vector), count, with_metadata:, _filter:)
      end

      def search_id(id, count: 5, with_metadata: false, _filter: nil)
        id = item_id(id)
        key = item_key(id)

        vector =
          if @json
            s = run_command("JSON.GET", key, "$.v")
            to_binary(JSON.parse(s)[0]) if s
          else
            run_command("HGET", key, "v")
          end

        unless vector
          raise Error, "Could not find item #{id}"
        end

        search_command(vector, count + 1, with_metadata:, _filter:).reject { |v| v[:id] == id }.first(count)
      end
      alias_method :nearest, :search_id

      def promote(alias_name)
        run_command("FT.ALIASUPDATE", index_name(alias_name), @index_name)
        nil
      end

      def drop
        drop_index
        drop_keys
      end

      private

      def index_name(name)
        if name.include?(":")
          raise ArgumentError, "invalid name"
        end

        "neighbor-idx-#{name}"
      end

      def check_dimensions(vector)
        if vector.size != @dimensions
          raise ArgumentError, "expected #{@dimensions} dimensions"
        end
      end

      def item_key(id)
        "#{@prefix}#{item_id(id)}"
      end

      def item_id(id)
        @int_ids ? Integer(id) : id.to_s
      end

      def search_command(blob, count, with_metadata:, _filter:)
        filter = _filter ? "(#{_filter})" : "*"
        return_args = with_metadata ? [] : ["RETURN", 1, "__v_score"]
        resp = run_command("FT.SEARCH", @index_name, "#{filter}=>[KNN #{count.to_i} @v $BLOB AS __v_score]", "PARAMS", "2", "BLOB", blob, *search_sort_args, *return_args, "DIALECT", "2")
        if resp.is_a?(Hash)
          parse_results_hash(resp, with_metadata:)
        else
          parse_results_array(resp, with_metadata:)
        end
      end

      def search_sort_args
        @search_sort_args ||= Redis.server_type == :valkey ? [] : ["SORTBY", "__v_score"]
      end

      def parse_results_hash(resp, with_metadata:)
        prefix_length = nil
        resp["results"].map do |result|
          key = result["id"]
          info = result["extra_attributes"]
          prefix_length ||= find_prefix_length(key)
          search_result(key, info, prefix_length, with_metadata:)
        end
      end

      def parse_results_array(resp, with_metadata:)
        prefix_length = nil
        resp.shift.times.map do |i|
          key, info = resp.shift(2)
          info = info.each_slice(2).to_h unless info.is_a?(Hash)
          prefix_length ||= find_prefix_length(key)
          search_result(key, info, prefix_length, with_metadata:)
        end
      end

      def search_result(key, info, prefix_length, with_metadata:)
        score = info["__v_score"].to_f
        distance = calculate_distance(score)

        result = {
          id: item_id(key[prefix_length..-1]),
          distance: distance
        }
        if with_metadata
          if @json
            result[:metadata] = JSON.parse(info["$"]).except("v")
          else
            result[:metadata] = info.except("v", "__v_score")
          end
        end
        result
      end

      def calculate_distance(score)
        case @distance_metric
        when "L2"
          Math.sqrt(score)
        when "IP"
          (score * -1) + 1
        else
          score
        end
      end

      # can't just remove @prefix since may be an alias
      def find_prefix_length(key)
        key[@global_prefix.length..-1].index(":") + @global_prefix.length + 1
      end

      def drop_index
        run_command("FT.DROPINDEX", @index_name)
      end

      def drop_keys
        cursor = 0
        begin
          cursor, keys = run_command("SCAN", cursor, "MATCH", "#{@prefix}*", "COUNT", 100)
          run_command("DEL", *keys) if keys.any?
        end while cursor != "0"
      end

      def to_binary(vector)
        vector.to_a.pack(pack_format)
      end

      def from_binary(s)
        s.unpack(pack_format)
      end

      def pack_format
        @pack_format ||= @float64 ? "d#{@dimensions}" : "f#{@dimensions}"
      end

      def hash_result(result)
        result.is_a?(Array) ? result.each_slice(2).to_h : result
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
