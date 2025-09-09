require_relative "test_helper"

class VectorSetTest < Minitest::Test
  def setup
    skip if server_version.to_i < 8
    super
    vector_set.drop
  end

  def test_new_invalid_name
    error = assert_raises(ArgumentError) do
      Neighbor::Redis::VectorSet.new("items:")
    end
    assert_equal "invalid name", error.message
  end

  def test_exists
    assert_equal false, vector_set.exists?
    vector_set.add(1, [1, 1, 1])
    assert_equal true, vector_set.exists?
  end

  def test_info
    vector_set.add(1, [1, 1, 1])
    info = vector_set.info
    assert_equal "f32", info[:quant_type]
    assert_equal 16, info[:hnsw_m]
    assert_equal 3, info[:vector_dim]
    assert_equal 0, info[:projection_input_dim]
    assert_equal 1, info[:size]
    assert_kind_of Integer, info[:max_level]
    assert_equal 0, info[:attributes_count]
    assert_kind_of Integer, info[:vset_uid]
    assert_equal 1, info[:hnsw_max_node_uid]
  end

  def test_info_missing
    assert_nil vector_set&.info
  end

  def test_dimensions
    assert_nil vector_set.dimensions
    vector_set.add(1, [1, 1, 1])
    assert_equal 3, vector_set.dimensions
  end

  def test_count
    add_items(vector_set)
    assert_equal 3, vector_set.count

    vector_set.remove(2)
    assert_equal 2, vector_set.count
  end

  def test_add
    assert_equal true, vector_set.add(1, [1, 1, 1])
    assert_equal false, vector_set.add(1, [1, 2, 3])
    assert_equal [1, 2, 3], vector_set.find(1)
  end

  def test_add_metadata
    assert_equal true, vector_set.add(1, [1, 1, 1], metadata: {category: "A"})
    assert_equal ({"category" => "A"}), vector_set.metadata(1)

    assert_equal false, vector_set.add(1, [2, 2, 2])
    assert_equal ({"category" => "A"}), vector_set.metadata(1)

    assert_equal false, vector_set.add(1, [3, 3, 3], metadata: {})
    assert_empty vector_set.metadata(1)
  end

  def test_add_different_dimensions
    vector_set.add(1, [1, 1, 1])
    error = assert_raises do
      vector_set.add(2, [1, 1])
    end
    assert_match "Vector dimension mismatch - got 2 but set has 3", error.message
  end

  def test_add_all
    assert_equal [true, true, true], vector_set.add_all([1, 2, 3], [[1, 1, 1], [-1, -1, -1], [1, 1, 0]])
    assert_equal [false, true], vector_set.add_all([1, 4], [[2, 2, 2], [3, 3, 3]])
    assert_equal 4, vector_set.count
  end

  def test_add_all_metadata
    ids = [1, 2, 3]
    vectors = [[1, 1, 1], [-1, -1, -1], [1, 1, 0]]
    metadata = [{category: "A"}, {category: "B"}, nil]
    assert_equal [true, true, true], vector_set.add_all(ids, vectors, metadata:)
    assert_equal ({"category" => "A"}), vector_set.metadata(1)
    assert_equal ({"category" => "B"}), vector_set.metadata(2)
    assert_nil vector_set.metadata(3)
  end

  def test_add_all_different_dimensions
    vector_set.add(1, [1, 1, 1])
    error = assert_raises do
      vector_set.add_all([1, 2], [[1, 1], [1, 1]])
    end
    assert_match "Vector dimension mismatch - got 2 but set has 3", error.message
    assert_equal 1, vector_set.count
  end

  def test_add_all_different_dimensions_vectors
    error = assert_raises(ArgumentError) do
      vector_set.add_all([1, 2], [[1, 1, 1], [1, 1]])
    end
    assert_equal "different dimensions", error.message
    assert_equal 0, vector_set.count
  end

  def test_add_all_different_sizes
    error = assert_raises(ArgumentError) do
      vector_set.add_all([1, 2], [[1, 1, 1]])
    end
    assert_equal "different sizes", error.message
    assert_equal 0, vector_set.count
  end

  def test_add_all_different_sizes_metadata
    error = assert_raises(ArgumentError) do
      vector_set.add_all([1, 2], [[1, 1, 1], [1, 1, 1]], metadata: [{}])
    end
    assert_equal "different sizes", error.message
    assert_equal 0, vector_set.count
  end

  def test_member
    add_items(vector_set)
    assert_equal true, vector_set.member?(2)
    assert_equal false, vector_set.member?(4)
  end

  def test_include
    add_items(vector_set)
    assert_equal true, vector_set.include?(2)
    assert_equal false, vector_set.include?(4)
  end

  def test_remove
    add_items(vector_set)
    assert_equal true, vector_set.remove(2)
    assert_equal false, vector_set.remove(4)
    assert_equal 2, vector_set.count
  end

  def test_remove_all
    add_items(vector_set)
    assert_equal [true, false], vector_set.remove_all([2, 4])
    assert_equal 2, vector_set.count
  end

  def test_find
    add_items(vector_set)
    assert_elements_in_delta [1, 1, 1], vector_set.find(1)
    assert_elements_in_delta [-1, -1, -1], vector_set.find(2)
    assert_elements_in_delta [1, 1, 2], vector_set.find(3)
    assert_nil vector_set.find(4)
  end

  def test_metadata
    vector_set.add(1, [1, 1, 1], metadata: {category: "A"})
    vector_set.add(2, [-1, -1, -1], metadata: {category: "B"})
    vector_set.add(3, [1, 1, 0])

    assert_equal ({"category" => "A"}), vector_set.metadata(1)
    assert_equal ({"category" => "B"}), vector_set.metadata(2)
    assert_nil vector_set.metadata(3)
    assert_nil vector_set.metadata(4)
  end

  def test_set_metadata
    vector_set.add(1, [1, 1, 1])
    assert_nil vector_set.metadata(1)

    assert_equal true, vector_set.set_metadata(1, {"category" => "A"})
    assert_equal ({"category" => "A"}), vector_set.metadata(1)

    assert_equal true, vector_set.set_metadata(1, {"quantity" => 2, "size" => 1.5})
    assert_equal ({"quantity" => 2, "size" => 1.5}), vector_set.metadata(1)

    assert_equal true, vector_set.set_metadata(1, {})
    assert_empty vector_set.metadata(1)
  end

  def test_set_metadata_missing
    vector_set.add(1, [1, 1, 1])
    assert_equal false, vector_set.set_metadata(2, {})
    assert_equal false, vector_set.set_metadata(2, {"category" => "A"})
    assert_equal 1, vector_set.count
  end

  def test_remove_metadata
    vector_set.add(1, [1, 1, 1], metadata: {"category" => "A"})
    assert_equal ({"category" => "A"}), vector_set.metadata(1)

    assert_equal true, vector_set.remove_metadata(1)
    assert_nil vector_set.metadata(1)

    assert_equal false, vector_set.remove_metadata(2)
  end

  def test_remove_metadata_missing
    vector_set.add(1, [1, 1, 1])
    assert_equal false, vector_set.remove_metadata(2)
    assert_equal 1, vector_set.count
  end

  def test_search
    add_items(vector_set)
    result = vector_set.search([1, 1, 1])
    assert_equal [1, 3, 2], result.map { |v| v[:id] }
    assert_elements_in_delta [0, 0.05719095841050148, 2], result.map { |v| v[:distance] }
  end

  def test_search_metadata
    vector_set.add(1, [1, 1, 1], metadata: {category: "A"})
    vector_set.add(2, [-1, -1, -1], metadata: {category: "B"})
    vector_set.add(3, [1, 1, 0])

    result = vector_set.search([1, 1, 1], with_metadata: true)
    assert_equal ({"category" => "A"}), result[0][:metadata]
    assert_empty result[1][:metadata]
    assert_equal ({"category" => "B"}), result[2][:metadata]

    result = vector_set.search([1, 1, 1], _filter: ".category == 'B'")
    assert_equal [2], result.map { |v| v[:id] }
  end

  def test_search_ef_search
    add_items(vector_set)
    result = vector_set.search([1, 1, 1], ef_search: 2)
    # still returns 3 results
    assert_equal [1, 3, 2], result.map { |v| v[:id] }
    assert_elements_in_delta [0, 0.05719095841050148, 2], result.map { |v| v[:distance] }
  end

  def test_search_exact
    add_items(vector_set)
    result = vector_set.search([1, 1, 1], exact: true)
    assert_equal [1, 3, 2], result.map { |v| v[:id] }
    assert_elements_in_delta [0, 0.05719095841050148, 2], result.map { |v| v[:distance] }
  end

  def test_search_different_dimensions
    add_items(vector_set)
    error = assert_raises do
      vector_set.search([1, 1])
    end
    assert_match "Vector dimension mismatch - got 2 but set has 3", error.message
  end

  def test_search_id
    add_items(vector_set)
    result = vector_set.search_id(1)
    assert_equal [3, 2], result.map { |v| v[:id] }
    assert_elements_in_delta [0.05719095841050148, 2], result.map { |v| v[:distance] }
  end

  def test_search_id_metadata
    vector_set.add(1, [1, 1, 1], metadata: {category: "A"})
    vector_set.add(2, [-1, -1, -1], metadata: {category: "B"})
    vector_set.add(3, [1, 1, 0])

    result = vector_set.search_id(1, with_metadata: true)
    assert_empty result[0][:metadata]
    assert_equal ({"category" => "B"}), result[1][:metadata]

    result = vector_set.search_id(1, _filter: ".category == 'B'")
    assert_equal [2], result.map { |v| v[:id] }
  end

  def test_search_id_exact
    add_items(vector_set)
    result = vector_set.search_id(1, exact: true)
    assert_equal [3, 2], result.map { |v| v[:id] }
    assert_elements_in_delta [0.05719095841050148, 2], result.map { |v| v[:distance] }
  end

  def test_search_id_missing
    add_items(vector_set)
    error = assert_raises do
      vector_set.search_id(4)
    end
    assert_match "element not found in set", error.message
  end

  def test_links
    add_items(vector_set)
    links = vector_set.links(1)
    assert_equal [2, 3], links.last.map { |v| v[:id] }
    assert_elements_in_delta [2, 0.05719095841050148], links.last.map { |v| v[:distance] }

    assert_nil vector_set.links(4)
  end

  def test_sample
    assert_nil vector_set.sample
    assert_empty vector_set.sample(3)

    add_items(vector_set)
    assert_includes [1, 2, 3], vector_set.sample
    assert_equal [1, 2, 3], vector_set.sample(3).sort
  end

  def test_drop
    vector_set.add(1, [1, 1, 1])
    assert_equal true, vector_set.drop
    assert_equal false, vector_set.drop
  end

  def test_options
    vector_set = Neighbor::Redis::VectorSet.new("items", m: 16, ef_construction: 200, ef_search: 10, epsilon: 0.5, id_type: "integer")
    add_items(vector_set)
    result = vector_set.search_id(1)
    assert_equal [3], result.map { |v| v[:id] }
    assert_elements_in_delta [0.05719095841050148], result.map { |v| v[:distance] }
  end

  def test_id_type_integer
    vector_set = Neighbor::Redis::VectorSet.new("items", id_type: "integer")
    vector_set.add(1, [1, 1, 1])
    vector_set.add("2", [-1, -1, -1])
    error = assert_raises(ArgumentError) do
      vector_set.add("3a", [1, 1, 0])
    end
    assert_match "invalid value for Integer()", error.message
    assert_equal [2], vector_set.search_id(1).map { |v| v[:id] }
    assert_equal [1, 2], vector_set.search([1, 1, 1]).map { |v| v[:id] }
    assert_equal [2], vector_set.links(1).last.map { |v| v[:id] }
    assert_equal [1, 2], vector_set.sample(2).sort
  end

  def test_id_type_string
    vector_set = Neighbor::Redis::VectorSet.new("items", id_type: "string")
    vector_set.add(1, [1, 1, 1])
    vector_set.add("2", [-1, -1, -1])
    assert_equal ["2"], vector_set.search_id(1).map { |v| v[:id] }
    assert_equal ["1", "2"], vector_set.search([1, 1, 1]).map { |v| v[:id] }
    assert_equal ["2"], vector_set.links(1).last.map { |v| v[:id] }
    assert_equal ["1", "2"], vector_set.sample(2).sort
  end

  def test_quantization_binary
    vector_set = Neighbor::Redis::VectorSet.new("items", quantization: "binary", id_type: "integer")
    vector_set.add(1, [1, 1, 1])
    vector_set.add(2, [-1, -1, -1])
    vector_set.add(3, [100, 10, 0])
    result = vector_set.search([1, 1, 1])
    assert_equal [1, 3, 2], result.map { |v| v[:id] }
    assert_elements_in_delta [0, 0.6666, 2], result.map { |v| v[:distance] }
    assert_equal [1, 1, 1], vector_set.find(1)
    assert_equal [-1, -1, -1], vector_set.find(2)
    assert_equal [1, 1, -1], vector_set.find(3)
    assert_equal "bin", vector_set.info[:quant_type]
  end

  def test_quantization_int8
    vector_set = Neighbor::Redis::VectorSet.new("items", quantization: "int8", id_type: "integer")
    vector_set.add(1, [1, 1, 1])
    vector_set.add(2, [-1, -1, -1])
    vector_set.add(3, [100, 10, 0])
    result = vector_set.search([1, 1, 1])
    assert_equal [1, 3, 2], result.map { |v| v[:id] }
    assert_elements_in_delta [0, 0.3677, 2], result.map { |v| v[:distance] }
    assert_elements_in_delta [1, 1, 1], vector_set.find(1)
    assert_elements_in_delta [-1, -1, -1], vector_set.find(2)
    assert_elements_in_delta [100, 10.236221313476562, 0], vector_set.find(3)
    assert_equal "int8", vector_set.info[:quant_type]
  end

  def test_reduce
    vector_set = Neighbor::Redis::VectorSet.new("items", reduce: 2)
    vector_set.add(1, [1, 1, 1])
    vector_set.add_all([2, 3], [[-1, -1, -1], [1, 1, 2]])
    assert_equal 2, vector_set.find(1).length
    assert_equal 2, vector_set.find(2).length
    assert_equal 2, vector_set.find(3).length
  end

  private

  def vector_set
    @vector_set ||= Neighbor::Redis::VectorSet.new("items", id_type: "integer")
  end

  def add_items(vector_set)
    ids = [1, 2, 3]
    vectors = [
      [1, 1, 1],
      [-1, -1, -1],
      [1, 1, 2]
    ]
    vector_set.add_all(ids, vectors)
  end
end
