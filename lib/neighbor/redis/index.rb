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
          "DISTANCE_METRIC" => @distance_metric,
        }.merge(create_params)

        command = ["FT.CREATE", @index_name]
        command.push("ON", "JSON") if @json
        command.push("PREFIX", "1", @prefix, "SCHEMA")
        command.push("$.v", "AS") if @json
        command.push("v", "VECTOR", @algorithm, params.size * 2, params)
        ft_command { redis.call(*command) }
      end

      def exists?
        redis.call("FT.INFO", @index_name)
        true
      rescue ArgumentError
        # fix for invalid value for Float(): "-nan"
        true
      rescue => e
        raise unless e.message.downcase.include?("unknown index name")
        false
      end

      def add(id, embedding)
        add_all([id], [embedding])
      end

      def add_all(ids, embeddings)
        ids = ids.to_a
        embeddings = embeddings.to_a

        raise ArgumentError, "different sizes" if ids.size != embeddings.size

        embeddings.each { |e| check_dimensions(e) }

        redis.pipelined do |pipeline|
          ids.zip(embeddings).each do |id, embedding|
            if @json
              pipeline.call("JSON.SET", item_key(id), "$", JSON.generate({v: embedding}))
            else
              pipeline.call("HSET", item_key(id), {v: to_binary(embedding)})
            end
          end
        end
      end

      def remove(id)
        remove_all([id])
      end

      def remove_all(ids)
        redis.call("DEL", ids.map { |id| item_key(id) })
      end

      def search(embedding, count: 5)
        check_dimensions(embedding)

        search_by_blob(to_binary(embedding), count)
      end

      def find(id)
        if @json
          s = redis.call("JSON.GET", item_key(id), "$.v")
          JSON.parse(s)[0] if s
        else
          from_binary(redis.call("HGET", item_key(id), "v"))
        end
      end

      def nearest(id, count: 5)
        embedding =
          if @json
            s = redis.call("JSON.GET", item_key(id), "$.v")
            to_binary(JSON.parse(s)[0]) if s
          else
            redis.call("HGET", item_key(id), "v")
          end

        unless embedding
          raise Error, "Could not find item #{id}"
        end

        search_by_blob(embedding, count + 1).reject { |v| v[:id] == id.to_s }.first(count)
      end

      def drop
        drop_index
        drop_keys
      end

      def promote(alias_name)
        redis.call("FT.ALIASUPDATE", index_name(alias_name), @index_name)
      end

      private

      def index_name(name)
        if name.include?(":")
          raise ArgumentError, "Invalid name"
        end

        "neighbor-idx-#{name}"
      end

      def check_dimensions(embedding)
        if embedding.size != @dimensions
          raise ArgumentError, "expected #{@dimensions} dimensions"
        end
      end

      def item_key(id)
        "#{@prefix}#{id}"
      end

      def search_by_blob(blob, count)
        resp = redis.call("FT.SEARCH", @index_name, "*=>[KNN #{count.to_i} @v $BLOB]", "PARAMS", "2", "BLOB", blob, "SORTBY", "__v_score", "DIALECT", "2")

        resp.is_a?(Hash) ? parse_results_hash(resp) : parse_results_array(resp)
      end

      def parse_results_hash(resp)
        resp["results"].each.map do |result|

          key = result["id"]
          info = result["extra_attributes"]
          score = info["__v_score"].to_f
          distance = calculate_distance(score)

          prefix_length ||= find_prefix_length(key)

          {
            id: key[prefix_length..-1],
            distance: distance
          }
        end
      end

      def parse_results_array(resp)
        len = resp.shift
        prefix_length = nil
        len.times.map do |i|
          key, info = resp.shift(2)
          info = info.each_slice(2).to_h
          score = info["__v_score"].to_f
          distance = calculate_distance(score)

          prefix_length ||= find_prefix_length(key)

          {
            id: key[prefix_length..-1],
            distance: distance
          }
        end
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
        redis.call("FT.DROPINDEX", @index_name)
      end

      def drop_keys
        cursor = 0
        begin
          cursor, keys = redis.call("SCAN", cursor, "MATCH", "#{@prefix}*", "COUNT", 100)
          redis.call("DEL", keys) if keys.any?
        end while cursor != "0"
      end

      def to_binary(embedding)
        embedding.to_a.pack(pack_format)
      end

      def from_binary(s)
        s.unpack(pack_format)
      end

      def pack_format
        @pack_format ||= @float64 ? "d#{@dimensions}" : "f#{@dimensions}"
      end

      # just use for create for now
      def ft_command
        yield
      rescue => e
        raise Error, "RediSearch not installed" if e.message.include?("ERR unknown command 'FT.")
        raise
      end

      def redis
        Redis.client
      end
    end
  end
end
