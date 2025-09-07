require "bundler/setup"
Bundler.require(:default)
require "minitest/autorun"
require "minitest/pride"

Neighbor::Redis.client =
  if defined?(Redis)
    Redis.new
  else
    RedisClient.config.new_pool
  end

module RedisInstrumentation
  def call(command, redis_config)
    puts "[redis] #{command.inspect}"
    super
  end

  def call_pipelined(commands, redis_config)
    puts "[redis] #{commands.inspect}"
    super
  end
end
RedisClient.register(RedisInstrumentation) if ENV["VERBOSE"]

class Minitest::Test
  def assert_elements_in_delta(expected, actual)
    assert_equal expected.size, actual.size
    expected.zip(actual) do |exp, act|
      assert_in_delta exp, act
    end
  end

  def server_version
    @@server_version ||= /redis_version:(\S+)/.match(redis.call("INFO"))[1]
  end

  def redis
    Neighbor::Redis.client
  end
end
