# Neighbor Redis

Nearest neighbor search for Ruby and Redis

[![Build Status](https://github.com/ankane/neighbor-redis/workflows/build/badge.svg?branch=master)](https://github.com/ankane/neighbor-redis/actions)

## Installation

First, [install RediSearch](https://redis.io/docs/stack/search/quick_start/). With Docker, use:

```sh
docker run -p 6379:6379 redis/redis-stack-server
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
index = Neighbor::Redis::HNSWIndex.new("items", dimensions: 3, distance: "l2")
index.create
```

Add items

```ruby
index.add(1, [1, 1, 1])
index.add(2, [2, 2, 2])
index.add(3, [1, 1, 2])
```

Note: IDs are stored and returned as strings (uses less total memory)

Get the nearest neighbors to an item

```ruby
index.nearest(1, count: 5)
```

Get the nearest neighbors to a vector

```ruby
index.search([1, 1, 1], count: 5)
```

## Distance

Supported values are:

- `l2`
- `inner_product`
- `cosine`

## Index Types

Hierarchical Navigable Small World (HNSW)

```ruby
Neighbor::Redis::HNSWIndex.new(
  name,
  initial_cap: nil,
  m: 16,
  ef_construction: 200,
  ef_runtime: 10,
  epsilon: 0.01
)
```

Flat

```ruby
Neighbor::Redis::FlatIndex.new(
  name,
  initial_cap: nil,
  block_size: 1024
)
```

## Additional Options

Store vectors as double precision (instead of single precision)

```ruby
Neighbor::Redis::HNSWIndex.new(name, type: "float64")
```

Store vectors as JSON (instead of a hash/blob)

```ruby
Neighbor::Redis::HNSWIndex.new(name, redis_type: "json")
```

## Changing Options

Create a new index to change any index options

```ruby
Neighbor::Redis::HNSWIndex.new("items-v2", **new_options)
```

## Additional Operations

Add multiple items

```ruby
index.add_all(ids, embeddings)
```

Get an item

```ruby
index.find(id)
```

Remove an item

```ruby
index.remove(id)
```

Remove multiple items

```ruby
index.remove_all(ids)
```

Drop the index

```ruby
index.drop
```

## Example

You can use Neighbor Redis for online item-based recommendations with [Disco](https://github.com/ankane/disco). We’ll use MovieLens data for this example.

Create an index

```ruby
index = Neighbor::Redis::HNSWIndex.new("movies", dimensions: 20, distance: "cosine")
index.create
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
index.nearest("Star Wars (1977)").map { |v| v[:id] }
```

See the [complete code](examples/disco_item_recs.rb)

## Reference

- [Vector similarity](https://redis.io/docs/stack/search/reference/vectors/)

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
