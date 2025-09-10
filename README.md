# Neighbor Redis

Nearest neighbor search for Ruby and Redis

Supports Redis 8 [vector sets](https://redis.io/docs/latest/develop/data-types/vector-sets/) and RediSearch [vector indexes](https://redis.io/docs/latest/develop/ai/search-and-query/vectors/)

[![Build Status](https://github.com/ankane/neighbor-redis/actions/workflows/build.yml/badge.svg)](https://github.com/ankane/neighbor-redis/actions)

## Installation

First, install Redis. With Docker, use:

```sh
docker run -p 6379:6379 redis:8
```

Add this line to your application’s Gemfile:

```ruby
gem "neighbor-redis"
```

And set the Redis client:

```ruby
Neighbor::Redis.client = RedisClient.config.new_pool
```

## Getting Started

Create an index

```ruby
index = Neighbor::Redis::VectorSet.new("items")
```

Add vectors

```ruby
index.add(1, [1, 1, 1])
index.add(2, [2, 2, 2])
index.add(3, [1, 1, 2])
```

Search for nearest neighbors to a vector

```ruby
index.search([1, 1, 1], count: 5)
```

Search for nearest neighbors to a vector in the index

```ruby
index.search_id(1, count: 5)
```

IDs are treated as strings by default, but can also be treated as integers

```ruby
Neighbor::Redis::VectorSet.new("items", id_type: "integer")
```

## Operations

Add or update a vector

```ruby
index.add(id, vector)
```

Add or update multiple vectors

```ruby
index.add_all(ids, vectors)
```

Get a vector

```ruby
index.find(id)
```

Remove a vector

```ruby
index.remove(id)
```

Remove multiple vectors

```ruby
index.remove_all(ids)
```

Count vectors

```ruby
index.count
```

## Metadata

Add a vector with metadata

```ruby
index.add(id, vector, metadata: {category: "A"})
```

Add multiple vectors with metadata

```ruby
index.add_all(ids, vectors, metadata: [{category: "A"}, {category: "B"}, ...])
```

Get metadata for a vector

```ruby
index.metadata(id)
```

Get metadata with search results

```ruby
index.search(vector, with_metadata: true)
```

Set metadata

```ruby
index.set_metadata(id, {category: "B"})
```

Remove metadata

```ruby
index.remove_metadata(id)
```

## Index Types

[Vector sets](#vector-sets)

- use cosine distance
- use single-precision floats
- support exact and approximate search
- support quantization and dimensionality reduction

[Vector indexes](#vector-indexes)

- support L2, inner product, and cosine distance
- support single or double-precision floats
- support either exact (flat) or approximate (HNSW and SVS Vamana) search
- can support quantization and dimensionality reduction (SVS Vamana)
- require calling `create` before adding vectors

## Vector Sets

Create a vector set

```ruby
Neighbor::Redis::VectorSet.new(name)
```

Specify parameters

```ruby
Neighbor::Redis::VectorSet.new(name, m: 16, ef_construction: 200, ef_search: 10)
```

Use int8 or binary quantization

```ruby
Neighbor::Redis::VectorSet.new(name, quantization: "int8")
# or
Neighbor::Redis::VectorSet.new(name, quantization: "binary")
```

Use dimensionality reduction

```ruby
Neighbor::Redis::VectorSet.new(name, reduce: 1)
```

Perform exact search

```ruby
index.search(vector, exact: true)
```

## Vector Indexes

Create a vector index

```ruby
index = Neighbor::Redis::HnswIndex.new("items", dimensions: 3, distance: "cosine")
index.create
```

Supports `l2`, `inner_product`, and `cosine` distance

Store vectors as double precision (instead of single precision)

```ruby
Neighbor::Redis::HnswIndex.new(name, type: "float64")
```

Store vectors as JSON (instead of a hash/blob)

```ruby
Neighbor::Redis::HnswIndex.new(name, redis_type: "json")
```

### Index Options

HNSW

```ruby
Neighbor::Redis::HnswIndex.new(name, m: 16, ef_construction: 200, ef_search: 10)
```

SVS Vamana - *Redis 8.2+*

```ruby
Neighbor::Redis::SvsVamanaIndex.new(
  name,
  compression: nil,
  construction_window_size: 200,
  graph_max_degree: 32,
  search_window_size: 10,
  training_threshold: nil,
  reduce: nil
)
```

Flat

```ruby
Neighbor::Redis::FlatIndex.new(name)
```

## Example

You can use Neighbor Redis for online item-based recommendations with [Disco](https://github.com/ankane/disco). We’ll use MovieLens data for this example.

Create an index

```ruby
index = Neighbor::Redis::VectorSet.new("movies")
```

Fit the recommender

```ruby
data = Disco.load_movielens
recommender = Disco::Recommender.new(factors: 20)
recommender.fit(data)
```

Store the item factors

```ruby
index.add_all(recommender.item_ids, recommender.item_factors)
```

And get similar movies

```ruby
index.search_id("Star Wars (1977)").map { |v| v[:id] }
```

See the complete code for [vector sets](examples/disco_item_recs_vs.rb) and [vector indexes](examples/disco_item_recs.rb)

## Reference

Get index info

```ruby
index.info
```

Check if an index exists

```ruby
index.exists?
```

Drop an index

```ruby
index.drop
```

## History

View the [changelog](CHANGELOG.md)

## Contributing

Everyone is encouraged to help improve this project. Here are a few ways you can help:

- [Report bugs](https://github.com/ankane/neighbor-redis/issues)
- Fix bugs and [submit pull requests](https://github.com/ankane/neighbor-redis/pulls)
- Write, clarify, or fix documentation
- Suggest or add new features

To get started with development:

```sh
git clone https://github.com/ankane/neighbor-redis.git
cd neighbor-redis
bundle install
bundle exec rake test
```
