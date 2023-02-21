require_relative "test_helper"

class IndexTest < Minitest::Test
  def setup
    index = Neighbor::Redis::Index.new("items", dimensions: 3, distance: "l2")
    index.drop if index.exists?
  end

  def test_l2
    index = create_index("l2")
    add_items(index)
    result = index.nearest(1)
    assert_equal [3, 2], result.map { |v| v[:id].to_i }
    assert_elements_in_delta [1, 1.7320507764816284], result.map { |v| v[:distance] }
  end

  def test_inner_product
    index = create_index("inner_product")
    add_items(index)
    result = index.nearest(1)
    assert_equal [2, 3], result.map { |v| v[:id].to_i }
    assert_elements_in_delta [6, 4], result.map { |v| v[:distance] }
  end

  def test_cosine
    index = create_index("cosine")
    add_items(index)
    result = index.nearest(1)
    assert_equal [2, 3], result.map { |v| v[:id].to_i }
    assert_elements_in_delta [0, 0.05719095841050148], result.map { |v| v[:distance] }
  end

  def test_add
    index = create_index("l2")
    3.times do |i|
      index.add(1, [i, i, i])
    end
    assert_equal [2, 2, 2], index.find(1)
  end

  def test_search
    index = create_index("l2")
    add_items(index)
    result = index.search([1, 1, 1])
    assert_equal [1, 3, 2], result.map { |v| v[:id].to_i }
    assert_elements_in_delta [0, 1, 1.7320507764816284], result.map { |v| v[:distance] }
  end

  def test_remove
    index = create_index("l2")
    add_items(index)
    index.remove(2)
    index.remove(4)
    assert_equal [1, 3], index.search([1, 1, 1]).map { |v| v[:id].to_i }
  end

  def test_remove_all
    index = create_index("l2")
    add_items(index)
    index.remove_all([2, 4])
    assert_equal [1, 3], index.search([1, 1, 1]).map { |v| v[:id].to_i }
  end

  def test_drop
    index = create_index("l2")
    assert_equal true, index.exists?
    index.drop
    assert_equal false, index.exists?
  end

  def test_flat
    index = Neighbor::Redis::FlatIndex.create("items", dimensions: 3, distance: "l2")
    add_items(index)
    assert_equal [1, 3, 2], index.search([1, 1, 1]).map { |v| v[:id].to_i }
  end

  def test_hnsw
    index = Neighbor::Redis::HNSWIndex.create("items", dimensions: 3, distance: "l2")
    add_items(index)
    assert_equal [1, 3, 2], index.search([1, 1, 1]).map { |v| v[:id].to_i }
  end

  def test_flat_json
    index = Neighbor::Redis::FlatIndex.create("items", dimensions: 3, distance: "l2", redis_type: "json")
    add_items(index)
    assert_equal [1, 3, 2], index.search([1, 1, 1]).map { |v| v[:id].to_i }
  end

  def test_hnsw_json
    index = Neighbor::Redis::HNSWIndex.create("items", dimensions: 3, distance: "l2", redis_type: "json")
    add_items(index)
    assert_equal [1, 3, 2], index.search([1, 1, 1]).map { |v| v[:id].to_i }
  end

  def test_float64
    index = create_index("l2", type: "float64")
    add_items(index)
    assert_equal [1, 3, 2], index.search([1, 1, 1]).map { |v| v[:id].to_i }
  end

  def test_float64_json
    index = create_index("l2", type: "float64", redis_type: "json")
    add_items(index)
    assert_equal [1, 3, 2], index.search([1, 1, 1]).map { |v| v[:id].to_i }
  end

  def test_promote
    index = create_index("l2")
    add_items(index)

    3.times do
      index.promote("new-items")
    end

    index = Neighbor::Redis::HNSWIndex.new("new-items", dimensions: 3, distance: "l2")
    assert_equal [1, 3, 2], index.search([1, 1, 1]).map { |v| v[:id].to_i }
  end

  def test_invalid_name
    error = assert_raises(ArgumentError) do
      Neighbor::Redis::HNSWIndex.create("items:", dimensions: 3, distance: "l2")
    end
    assert_equal "Invalid name", error.message
  end

  def test_index_exists
    index = create_index("l2")

    error = assert_raises do
      index.create
    end
    assert_equal "Index already exists", error.message
  end

  def test_nearest_missing
    index = create_index("l2")
    error = assert_raises(Neighbor::Redis::Error) do
      index.nearest(4)
    end
    assert_equal "Could not find item 4", error.message
  end

  def test_find_missing
    index = create_index("l2")
    error = assert_raises(Neighbor::Redis::Error) do
      index.nearest(4)
    end
    assert_equal "Could not find item 4", error.message
  end

  def test_add_invalid_dimensions
    index = create_index("l2")
    error = assert_raises(ArgumentError) do
      index.add(4, [1, 2])
    end
    assert_equal "expected 3 dimensions", error.message
  end

  def test_add_all_invalid_dimensions
    index = create_index("l2")
    error = assert_raises(ArgumentError) do
      index.add_all([4], [[1, 2]])
    end
    assert_equal "expected 3 dimensions", error.message
  end

  def test_add_all_different_sizes
    index = create_index("l2")
    error = assert_raises(ArgumentError) do
      index.add_all([1, 2], [[1, 2, 3]])
    end
    assert_equal "different sizes", error.message
  end

  def test_search_invalid_dimensions
    index = create_index("l2")
    error = assert_raises(ArgumentError) do
      index.search([1, 2])
    end
    assert_equal "expected 3 dimensions", error.message
  end

  private

  def create_index(distance, **options)
    Neighbor::Redis::HNSWIndex.create("items", dimensions: 3, distance: distance, **options)
  end

  def add_items(index)
    ids = [1, 2, 3]
    vectors = [
      [1, 1, 1],
      [2, 2, 2],
      [1, 1, 2]
    ]
    index.add_all(ids, vectors)
  end
end
