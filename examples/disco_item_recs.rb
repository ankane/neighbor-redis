require "disco"
require "neighbor-redis"

Neighbor::Redis.client = RedisClient.config.new_pool

index = Neighbor::Redis::HNSWIndex.new("movies", dimensions: 20, distance: "cosine")
index.drop if index.exists?
index.create

data = Disco.load_movielens
recommender = Disco::Recommender.new(factors: 20)
recommender.fit(data)

# recommender.item_ids.each do |item_id|
#   index.add(item_id, recommender.item_factors(item_id))
# end

index.add_all(recommender.item_ids, recommender.item_factors)

pp index.nearest("Star Wars (1977)").map { |v| v[:id] }
