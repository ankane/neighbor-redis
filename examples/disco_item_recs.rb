require "disco"
require "neighbor-redis"

Neighbor::Redis.client = RedisClient.config.new_pool

index = Neighbor::Redis::HNSWIndex.new("movies", dimensions: 20, distance: "cosine")
index.drop if index.exists?
index.create

data = Disco.load_movielens
recommender = Disco::Recommender.new(factors: 20)
recommender.fit(data)

index.add_all(recommender.item_ids, recommender.item_factors)

pp index.nearest("Star Wars (1977)").map { |v| v[:id] }
