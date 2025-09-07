module Neighbor
  module Redis
    class Index
      def initialize(name, dimensions:, distance:, type: "float32", redis_type: "hash")
        @index_name = index_name(name)
        @global_prefix = "neighbor:items:"
        @prefix = "#{@global_prefix}#{name}:"

        @dimensions = dimensions

        unless distance.nil?
          @distance_metric =
            case distance.to_s
            when "l2", "cosine"
              distance.to_s.upcase
            when "inner_product"
              "IP"
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
      end

      def self.create(...)
        index = new(...)
        index.create
        index
      end

      def create
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
        run_command(*command)
        nil
      end

      def exists?
        run_command("FT.INFO", @index_name)
        true
      rescue ArgumentError
        # fix for invalid value for Float(): "-nan"
        true
      rescue => e
        message = e.message.downcase
        raise unless message.include?("unknown index name") || message.include?("no such index")
        false
      end

      # TODO fix nested
      # TODO symbolize keys?
      def info
        hash_result(run_command("FT.INFO", @index_name))
      end

      def count
        info["num_docs"]
      end

      def add(id, vector)
        add_all([id], [vector])[0]
      end

      def add_all(ids, vectors)
        ids = ids.to_a
        vectors = vectors.to_a

        raise ArgumentError, "different sizes" if ids.size != vectors.size

        vectors.each { |e| check_dimensions(e) }

        result =
          redis.pipelined do |pipeline|
            ids.zip(vectors).each do |id, vector|
              if @json
                pipeline.call("JSON.SET", item_key(id), "$", JSON.generate({v: vector}))
              else
                pipeline.call("HSET", item_key(id), {v: to_binary(vector)})
              end
            end
          end
        result.map { |v| v == 1 }
      end

      def remove(id)
        remove_all([id]) == 1
      end

      def remove_all(ids)
        run_command("DEL", *ids.map { |id| item_key(id) })
      end

      def search(vector, count: 5)
        check_dimensions(vector)

        search_by_blob(to_binary(vector), count)
      end

      def find(id)
        if @json
          s = run_command("JSON.GET", item_key(id), "$.v")
          JSON.parse(s)[0] if s
        else
          s = run_command("HGET", item_key(id), "v")
          from_binary(s) if s
        end
      end

      def nearest(id, count: 5)
        vector =
          if @json
            s = run_command("JSON.GET", item_key(id), "$.v")
            to_binary(JSON.parse(s)[0]) if s
          else
            run_command("HGET", item_key(id), "v")
          end

        unless vector
          raise Error, "Could not find item #{id}"
        end

        search_by_blob(vector, count + 1).reject { |v| v[:id] == id.to_s }.first(count)
      end

      def drop
        drop_index
        drop_keys
      end

      def promote(alias_name)
        run_command("FT.ALIASUPDATE", index_name(alias_name), @index_name)
        nil
      end

      private

      def index_name(name)
        if name.include?(":")
          raise ArgumentError, "Invalid name"
        end

        "neighbor-idx-#{name}"
      end

      def check_dimensions(vector)
        if vector.size != @dimensions
          raise ArgumentError, "expected #{@dimensions} dimensions"
        end
      end

      def item_key(id)
        "#{@prefix}#{id}"
      end

      def search_by_blob(blob, count)
        resp = run_command("FT.SEARCH", @index_name, "*=>[KNN #{count.to_i} @v $BLOB]", "PARAMS", "2", "BLOB", blob, "SORTBY", "__v_score", "DIALECT", "2")
        resp.is_a?(Hash) ? parse_results_hash(resp) : parse_results_array(resp)
      end

      def parse_results_hash(resp)
        prefix_length = nil
        resp["results"].map do |result|
          key = result["id"]
          info = result["extra_attributes"]
          prefix_length ||= find_prefix_length(key)
          search_result(key, info, prefix_length)
        end
      end

      def parse_results_array(resp)
        prefix_length = nil
        resp.shift.times.map do |i|
          key, info = resp.shift(2)
          info = info.each_slice(2).to_h
          prefix_length ||= find_prefix_length(key)
          search_result(key, info, prefix_length)
        end
      end

      def search_result(key, info, prefix_length)
        score = info["__v_score"].to_f
        distance = calculate_distance(score)

        {
          id: key[prefix_length..-1],
          distance: distance
        }
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
        if args.any? { |v| !(v.is_a?(String) || v.is_a?(Integer)) }
          raise TypeError, "Unexpected argument type"
        end
        redis.call(*args)
      rescue => e
        raise Error, "RediSearch not installed" if e.message.include?("ERR unknown command 'FT.")
        raise e
      end

      def redis
        Redis.client
      end
    end
  end
end
