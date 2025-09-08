require_relative "test_helper"

class IndexTest < Minitest::Test
  def setup
    super
    index = Neighbor::Redis::Index.new("items", dimensions: 3, distance: "l2")
    index.drop if index.exists?
  end

  def test_new_invalid_name
    error = assert_raises(ArgumentError) do
      Neighbor::Redis::HnswIndex.create("items:", dimensions: 3, distance: "l2")
    end
    assert_equal "invalid name", error.message
  end

  def test_create
    index = Neighbor::Redis::HnswIndex.new("items", dimensions: 3, distance: "l2")
    assert_nil index.create
  end

  def test_create_invalid_name
    error = assert_raises(ArgumentError) do
      Neighbor::Redis::HnswIndex.create("items:", dimensions: 3, distance: "l2")
    end
    assert_equal "invalid name", error.message
  end

  def test_create_exists
    index = create_index

    error = assert_raises do
      index.create
    end
    assert_match "already exists", error.message
  end

  def test_exists
    index = Neighbor::Redis::HnswIndex.new("items", dimensions: 3, distance: "l2")
    assert_equal false, index.exists?
    index.create
    assert_equal true, index.exists?
  end

  def test_info
    index = create_index
    info = index.info
    assert_kind_of Hash, info
  end

  def test_info_missing
    index = Neighbor::Redis::HnswIndex.new("items", dimensions: 3, distance: "l2")
    error = assert_raises do
      index.info
    end
    assert_match(/no such index|Unknown Index name|not found/, error.message)
  end

  def test_count
    index = create_index
    add_items(index)
    assert_equal 3, index.count

    index.remove(2)
    assert_equal 2, index.count
  end

  def test_add
    index = create_index
    assert_equal true, index.add(1, [1, 1, 1])
    assert_equal false, index.add(1, [2, 2, 2])
    assert_equal [2, 2, 2], index.find(1)
  end

  def test_add_different_dimensions
    index = create_index
    error = assert_raises(ArgumentError) do
      index.add(4, [1, 2])
    end
    assert_equal "expected 3 dimensions", error.message
  end

  def test_add_all
    index = create_index
    assert_equal [true, true], index.add_all([1, 2], [[1, 1, 1], [2, 2, 2]])
    assert_equal [false, true], index.add_all([1, 3], [[1, 1, 1], [1, 1, 2]])
  end

  def test_add_all_different_dimensions
    index = create_index
    error = assert_raises(ArgumentError) do
      index.add_all([4], [[1, 2]])
    end
    assert_equal "expected 3 dimensions", error.message
  end

  def test_add_all_different_sizes
    index = create_index
    error = assert_raises(ArgumentError) do
      index.add_all([1, 2], [[1, 2, 3]])
    end
    assert_equal "different sizes", error.message
  end

  def test_remove
    index = create_index(distance: "l2", id_type: "integer")
    add_items(index)
    assert_equal true, index.remove(2)
    assert_equal false, index.remove(4)
    assert_equal 2, index.count
    assert_equal [1, 3], index.search([1, 1, 1]).map { |v| v[:id] }
  end

  def test_remove_all
    index = create_index(distance: "l2", id_type: "integer")
    add_items(index)
    assert_equal 1, index.remove_all([2, 4])
    assert_equal 2, index.count
    assert_equal [1, 3], index.search([1, 1, 1]).map { |v| v[:id] }
  end

  def test_find
    index = create_index
    add_items(index)
    assert_elements_in_delta [1, 1, 1], index.find(1)
    assert_elements_in_delta [2, 2, 2], index.find(2)
    assert_elements_in_delta [1, 1, 2], index.find(3)
    assert_nil index.find(4)
  end

  def test_search_l2
    index = create_index(distance: "l2", id_type: "integer")
    add_items(index)
    result = index.search([1, 1, 1])
    assert_equal [1, 3, 2], result.map { |v| v[:id] }
    assert_elements_in_delta [0, 1, 1.7320507764816284], result.map { |v| v[:distance] }
  end

  def test_search_inner_product
    index = create_index(distance: "inner_product", id_type: "integer")
    add_items(index)
    result = index.search([1, 1, 1])
    assert_equal [2, 3, 1], result.map { |v| v[:id] }
    assert_elements_in_delta [6, 4, 3], result.map { |v| v[:distance] }
  end

  def test_search_cosine
    index = create_index(distance: "cosine", id_type: "integer")
    index.add(1, [1, 1, 1])
    index.add(2, [-1, -1, -1])
    index.add(3, [1, 1, 2])
    result = index.search([1, 1, 1])
    assert_equal [1, 3, 2], result.map { |v| v[:id] }
    assert_elements_in_delta [0, 0.05719095841050148, 2], result.map { |v| v[:distance] }
  end

  def test_search_different_dimensions
    index = create_index
    error = assert_raises(ArgumentError) do
      index.search([1, 2])
    end
    assert_equal "expected 3 dimensions", error.message
  end

  def test_search_id_l2
    index = create_index(distance: "l2", id_type: "integer")
    add_items(index)
    result = index.search_id(1)
    assert_equal [3, 2], result.map { |v| v[:id] }
    assert_elements_in_delta [1, 1.7320507764816284], result.map { |v| v[:distance] }
  end

  def test_search_id_inner_product
    index = create_index(distance: "inner_product", id_type: "integer")
    add_items(index)
    result = index.search_id(1)
    assert_equal [2, 3], result.map { |v| v[:id] }
    assert_elements_in_delta [6, 4], result.map { |v| v[:distance] }
  end

  def test_search_id_cosine
    index = create_index(distance: "cosine", id_type: "integer")
    add_items(index)
    result = index.search_id(1)
    assert_equal [2, 3], result.map { |v| v[:id] }
    assert_elements_in_delta [0, 0.05719095841050148], result.map { |v| v[:distance] }
  end

  def test_search_id_missing
    index = create_index
    error = assert_raises(Neighbor::Redis::Error) do
      index.search_id(4)
    end
    assert_equal "Could not find item 4", error.message
  end

  def test_flat
    index = Neighbor::Redis::FlatIndex.create("items", dimensions: 3, distance: "l2", id_type: "integer")
    add_items(index)
    assert_equal [1, 3, 2], index.search([1, 1, 1]).map { |v| v[:id] }
  end

  def test_flat_json
    index = Neighbor::Redis::FlatIndex.create("items", dimensions: 3, distance: "l2", redis_type: "json", id_type: "integer")
    add_items(index)
    assert_equal [1, 3, 2], index.search([1, 1, 1]).map { |v| v[:id] }
  end

  def test_flat_float64
    skip unless supports_float64?

    index = Neighbor::Redis::FlatIndex.create("items", dimensions: 3, distance: "l2", type: "float64", id_type: "integer")
    add_items(index)
    assert_equal [1, 3, 2], index.search([1, 1, 1]).map { |v| v[:id] }
  end

  def test_flat_float64_json
    skip unless supports_float64?

    index = Neighbor::Redis::FlatIndex.create("items", dimensions: 3, distance: "l2", type: "float64", redis_type: "json", id_type: "integer")
    add_items(index)
    assert_equal [1, 3, 2], index.search([1, 1, 1]).map { |v| v[:id] }
  end

  def test_hnsw
    index = Neighbor::Redis::HnswIndex.create("items", dimensions: 3, distance: "l2", id_type: "integer")
    add_items(index)
    assert_equal [1, 3, 2], index.search([1, 1, 1]).map { |v| v[:id] }
  end

  def test_hnsw_json
    index = Neighbor::Redis::HnswIndex.create("items", dimensions: 3, distance: "l2", redis_type: "json", id_type: "integer")
    add_items(index)
    assert_equal [1, 3, 2], index.search([1, 1, 1]).map { |v| v[:id] }
  end

  def test_hnsw_float64
    skip unless supports_float64?

    index = Neighbor::Redis::HnswIndex.create("items", dimensions: 3, distance: "l2", type: "float64", id_type: "integer")
    add_items(index)
    assert_equal [1, 3, 2], index.search([1, 1, 1]).map { |v| v[:id] }
  end

  def test_hnsw_float64_json
    skip unless supports_float64?

    index = Neighbor::Redis::HnswIndex.create("items", dimensions: 3, distance: "l2", type: "float64", redis_type: "json", id_type: "integer")
    add_items(index)
    assert_equal [1, 3, 2], index.search([1, 1, 1]).map { |v| v[:id] }
  end

  def test_svs_vamana
    skip unless supports_svs_vamana?

    index = Neighbor::Redis::SvsVamanaIndex.create("items", dimensions: 3, distance: "l2", id_type: "integer")
    add_items(index)
    assert_equal [1, 3, 2], index.search([1, 1, 1]).map { |v| v[:id] }
  end

  def test_svs_vamana_json
    skip unless supports_svs_vamana?

    index = Neighbor::Redis::SvsVamanaIndex.create("items", dimensions: 3, distance: "l2", redis_type: "json", id_type: "integer")
    add_items(index)
    assert_equal [1, 3, 2], index.search([1, 1, 1]).map { |v| v[:id] }
  end

  def test_svs_vamana_float64
    skip unless supports_svs_vamana?

    error = assert_raises do
      Neighbor::Redis::SvsVamanaIndex.create("items", dimensions: 3, distance: "l2", type: "float64", id_type: "integer")
    end
    assert_match "Not supported data type is given. Expected: FLOAT16, FLOAT32", error.message
  end

  def test_promote
    skip if valkey?

    index = create_index(distance: "l2")
    add_items(index)

    3.times do
      assert_nil index.promote("new-items")
    end

    index = Neighbor::Redis::HnswIndex.new("new-items", dimensions: 3, distance: "l2", id_type: "integer")
    assert_equal [1, 3, 2], index.search([1, 1, 1]).map { |v| v[:id] }
  end

  def test_drop
    index = create_index
    assert_equal true, index.exists?
    assert_nil index.drop
    assert_equal false, index.exists?

    error = assert_raises do
      index.drop
    end
    assert_match(/no such index|Unknown Index name|not found/, error.message)
  end

  def test_id_type_integer
    index = create_index(distance: "l2", id_type: "integer")
    index.add(1, [1, 1, 1])
    index.add("2", [-1, -1, -1])
    error = assert_raises(ArgumentError) do
      index.add("3a", [1, 1, 0])
    end
    assert_match "invalid value for Integer()", error.message
    assert_equal [2], index.search_id(1).map { |v| v[:id] }
    assert_equal [1, 2], index.search([1, 1, 1]).map { |v| v[:id] }
  end

  def test_id_type_string
    index = create_index(distance: "l2", id_type: "string")
    index.add(1, [1, 1, 1])
    index.add("2", [-1, -1, -1])
    assert_equal ["2"], index.search_id(1).map { |v| v[:id] }
    assert_equal ["1", "2"], index.search([1, 1, 1]).map { |v| v[:id] }
  end

  private

  def create_index(**options)
    options[:distance] ||= ["l2", "inner_product", "cosine"].sample
    Neighbor::Redis::HnswIndex.create("items", dimensions: 3, **options)
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

  def supports_svs_vamana?
    server_version.to_f >= 8.2
  end

  def supports_float64?
    !valkey?
  end
end
