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

  def test_add_metadata
    index = create_index
    assert_equal true, index.add(1, [1, 1, 1], metadata: {category: "A"})
    assert_equal ({"category" => "A"}), index.metadata(1)

    assert_equal false, index.add(1, [2, 2, 2])
    assert_equal ({"category" => "A"}), index.metadata(1)

    assert_equal false, index.add(1, [3, 3, 3], metadata: {})
    # TODO fix
    # assert_empty index.metadata(1)
    assert_equal ({"category" => "A"}), index.metadata(1)

    error = assert_raises(ArgumentError) do
      index.add(1, [4, 4, 4], metadata: {v: [4, 4, 4]})
    end
    assert_equal "invalid metadata", error.message
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
      index.add_all([1, 4], [[1, 1, 1], [1, 2]])
    end
    assert_equal "expected 3 dimensions", error.message
    assert_equal 0, index.count
  end

  def test_add_all_different_sizes
    index = create_index
    error = assert_raises(ArgumentError) do
      index.add_all([1, 2], [[1, 2, 3]])
    end
    assert_equal "different sizes", error.message
    assert_equal 0, index.count
  end

  def test_member
    index = create_index
    add_items(index)
    assert_equal true, index.member?(2)
    assert_equal false, index.member?(4)
  end

  def test_include
    index = create_index(redis_type: "json")
    add_items(index)
    assert_equal true, index.include?(2)
    assert_equal false, index.include?(4)
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

  def test_metadata
    index = create_index
    index.add(1, [1, 1, 1], metadata: {category: "A"})
    index.add(2, [-1, -1, -1], metadata: {category: "B"})
    index.add(3, [1, 1, 0])

    assert_equal ({"category" => "A"}), index.metadata(1)
    assert_equal ({"category" => "B"}), index.metadata(2)
    assert_empty index.metadata(3)
    assert_nil index.metadata(4)
  end

  def test_metadata_json
    index = create_index(redis_type: "json")
    index.add(1, [1, 1, 1], metadata: {category: "A"})
    index.add(2, [-1, -1, -1], metadata: {category: "B"})
    index.add(3, [1, 1, 0])

    assert_equal ({"category" => "A"}), index.metadata(1)
    assert_equal ({"category" => "B"}), index.metadata(2)
    assert_empty index.metadata(3)
    assert_nil index.metadata(4)
  end

  def test_set_metadata
    index = create_index
    index.add(1, [1, 1, 1])
    assert_empty index.metadata(1)

    assert_equal true, index.set_metadata(1, {"category" => "A"})
    assert_equal ({"category" => "A"}), index.metadata(1)
    assert_equal [1, 1, 1], index.find(1)

    assert_equal true, index.set_metadata(1, {"quantity" => 2, "size" => 1.5})
    # TODO fix
    # assert_equal ({"quantity" => "2", "size" => "1.5"}), index.metadata(1)
    assert_equal ({"category" => "A", "quantity" => "2", "size" => "1.5"}), index.metadata(1)

    # TODO fix
    # assert_equal true, index.set_metadata(1, {})
    assert_equal false, index.set_metadata(1, {})
    # TODO fix
    # assert_empty index.metadata(1)
    assert_equal ({"category" => "A", "quantity" => "2", "size" => "1.5"}), index.metadata(1)

    error = assert_raises(ArgumentError) do
      index.set_metadata(1, {v: [1, 1, 1]})
    end
    assert_equal "invalid metadata", error.message
  end

  def test_set_metadata_json
    skip if server_version.to_i < 8 || valkey?

    index = create_index(redis_type: "json")
    index.add(1, [1, 1, 1])
    assert_empty index.metadata(1)

    assert_equal true, index.set_metadata(1, {"category" => "A"})
    assert_equal ({"category" => "A"}), index.metadata(1)
    assert_equal [1, 1, 1], index.find(1)

    assert_equal true, index.set_metadata(1, {"quantity" => 2, "size" => 1.5})
    # TODO fix
    # assert_equal ({"quantity" => 2, "size" => 1.5}), index.metadata(1)
    assert_equal ({"category" => "A", "quantity" => 2, "size" => 1.5}), index.metadata(1)

    assert_equal true, index.set_metadata(1, {})
    # TODO fix
    # assert_empty index.metadata(1)
    assert_equal ({"category" => "A", "quantity" => 2, "size" => 1.5}), index.metadata(1)

    error = assert_raises(ArgumentError) do
      index.set_metadata(1, {v: [1, 1, 1]})
    end
    assert_equal "invalid metadata", error.message
  end

  def test_set_metadata_missing
    skip if server_version.to_i < 8 || valkey?

    index = create_index(redis_type: "json")
    assert_equal false, index.set_metadata(2, {})
    assert_equal false, index.set_metadata(2, {"category" => "A"})
    assert_equal 0, index.count
  end

  def test_set_metadata_missing_json
    skip if server_version.to_i < 8 || valkey?

    index = create_index
    assert_equal false, index.set_metadata(2, {})
    assert_equal false, index.set_metadata(2, {"category" => "A"})
    assert_equal 0, index.count
  end

  def test_remove_metadata
    index = create_index
    index.add(1, [1, 1, 1], metadata: {"category" => "A"})
    assert_equal ({"category" => "A"}), index.metadata(1)

    assert_equal true, index.remove_metadata(1)
    assert_empty index.metadata(1)

    assert_equal false, index.remove_metadata(2)
  end

  def test_remove_metadata_json
    skip if server_version.to_i < 8 || valkey?

    index = create_index(redis_type: "json")
    index.add(1, [1, 1, 1], metadata: {"category" => "A"})
    assert_equal ({"category" => "A"}), index.metadata(1)

    assert_equal true, index.remove_metadata(1)
    assert_empty index.metadata(1)

    assert_equal false, index.remove_metadata(2)
  end

  def test_remove_metadata_missing
    index = create_index
    assert_equal false, index.remove_metadata(2)
    assert_equal 0, index.count
  end

  def test_remove_metadata_missing_json
    index = create_index(redis_type: "json")
    assert_equal false, index.remove_metadata(2)
    assert_equal 0, index.count
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
    skip if dragonfly?

    index = create_index(distance: "cosine", id_type: "integer")
    index.add(1, [1, 1, 1])
    index.add(2, [-1, -1, -1])
    index.add(3, [1, 1, 2])
    result = index.search([1, 1, 1])
    assert_equal [1, 3, 2], result.map { |v| v[:id] }
    assert_elements_in_delta [0, 0.05719095841050148, 2], result.map { |v| v[:distance] }
  end

  def test_search_metadata
    index = create_index(distance: "cosine")
    index.add(1, [1, 1, 1], metadata: {category: "A"})
    index.add(2, [-1, -1, -1], metadata: {category: "B"})
    index.add(3, [1, 1, 0])

    result = index.search([1, 1, 1], with_metadata: true)
    assert_equal ({"category" => "A"}), result[0][:metadata]
    assert_empty result[1][:metadata]
    assert_equal ({"category" => "B"}), result[2][:metadata]
  end

  def test_search_metadata_json
    index = create_index(distance: "cosine", redis_type: "json")
    index.add(1, [1, 1, 1], metadata: {category: "A"})
    index.add(2, [-1, -1, -1], metadata: {category: "B"})
    index.add(3, [1, 1, 0])

    result = index.search([1, 1, 1], with_metadata: true)
    assert_equal ({"category" => "A"}), result[0][:metadata]
    assert_empty result[1][:metadata]
    assert_equal ({"category" => "B"}), result[2][:metadata]
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
    skip if dragonfly?

    index = create_index(distance: "cosine", id_type: "integer")
    add_items(index)
    result = index.search_id(1)
    assert_equal [2, 3], result.map { |v| v[:id] }
    assert_elements_in_delta [0, 0.05719095841050148], result.map { |v| v[:distance] }
  end

  def test_search_id_metadata
    index = create_index(distance: "cosine")
    index.add(1, [1, 1, 1], metadata: {category: "A"})
    index.add(2, [-1, -1, -1], metadata: {category: "B"})
    index.add(3, [1, 1, 0])

    result = index.search_id(1, with_metadata: true)
    assert_empty result[0][:metadata]
    assert_equal ({"category" => "B"}), result[1][:metadata]
  end

  def test_search_id_metadata_json
    index = create_index(distance: "cosine", redis_type: "json")
    index.add(1, [1, 1, 1], metadata: {category: "A"})
    index.add(2, [-1, -1, -1], metadata: {category: "B"})
    index.add(3, [1, 1, 0])

    result = index.search_id(1, with_metadata: true)
    assert_empty result[0][:metadata]
    assert_equal ({"category" => "B"}), result[1][:metadata]
  end

  def test_search_id_missing
    index = create_index
    error = assert_raises(Neighbor::Redis::Error) do
      index.search_id(4)
    end
    assert_equal "Could not find item 4", error.message
  end

  def test_search_id_missing_json
    index = create_index(redis_type: "json")
    error = assert_raises(Neighbor::Redis::Error) do
      index.search_id(4)
    end
    assert_equal "Could not find item 4", error.message
  end

  def test_json
    index = create_index(redis_type: "json")
    assert_equal true, index.add(1, [1, 1, 1])
    # always returns true
    assert_equal true, index.add(1, [1, 1, 1])
    assert_equal true, index.remove(1)
    assert_equal false, index.remove(1)
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
    skip if valkey? || dragonfly?

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
    options[:distance] ||= ["l2", "inner_product", *(dragonfly? ? [] : ["cosine"])].sample
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
    !valkey? && !dragonfly?
  end
end
