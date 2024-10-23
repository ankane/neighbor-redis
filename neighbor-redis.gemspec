require_relative "lib/neighbor/redis/version"

Gem::Specification.new do |spec|
  spec.name          = "neighbor-redis"
  spec.version       = Neighbor::Redis::VERSION
  spec.summary       = "Nearest neighbor search for Ruby and Redis"
  spec.homepage      = "https://github.com/ankane/neighbor-redis"
  spec.license       = "MIT"

  spec.author        = "Andrew Kane"
  spec.email         = "andrew@ankane.org"

  spec.files         = Dir["*.{md,txt}", "{lib}/**/*"]
  spec.require_path  = "lib"

  spec.required_ruby_version = ">= 3.1"

  spec.add_dependency "redis-client"
end
