require "disco"
require "neighbor-redis"

Neighbor::Redis.client = RedisClient.config.new_pool

movies_index = Neighbor::Redis::HNSWIndex.new("movies", dimensions: 20, distance: "inner_product")
movies_index.drop if movies_index.exists?
movies_index.create

users_index = Neighbor::Redis::HNSWIndex.new("users", dimensions: 20, distance: nil)
users_index.drop if users_index.exists?
# no need to call create since not searching

data = Disco.load_movielens
recommender = Disco::Recommender.new(factors: 20)
recommender.fit(data)

movies_index.add_all(recommender.item_ids, recommender.item_factors)
users_index.add_all(recommender.user_ids, recommender.user_factors)

user_factors = users_index.find(123)
pp movies_index.search(user_factors).map { |v| v[:id] }
