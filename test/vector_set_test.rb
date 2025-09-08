require_relative "test_helper"

class VectorSetTest < Minitest::Test
  def setup
    skip if server_version.to_i < 8
    super
    vector_set.drop
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

  def test_add_all_different_dimensions
    error = assert_raises do
      vector_set.add_all([1, 2], [[1, 1, 1], [1, 1]])
    end
    assert_match "Vector dimension mismatch - got 2 but set has 3", error.message

    # non-atomic
    assert_equal 1, vector_set.count
  end

  def test_add_all_different_sizes
    error = assert_raises(ArgumentError) do
      vector_set.add_all([1, 2], [[1, 1, 1]])
    end
    assert_equal "different sizes", error.message
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
    assert_elements_in_delta [1, 1, 0], vector_set.find(3)
    assert_nil vector_set.find(4)
  end

  def test_attributes
    vector_set.add(1, [1, 1, 1], attributes: {category: "A"})
    vector_set.add(2, [-1, -1, -1], attributes: {category: "B"})
    vector_set.add(3, [1, 1, 0])

    assert_equal ({"category" => "A"}), vector_set.attributes(1)
    assert_equal ({"category" => "B"}), vector_set.attributes(2)
    assert_nil vector_set.attributes(3)
    assert_nil vector_set.attributes(4)
  end

  def test_update_attributes
    vector_set.add(1, [1, 1, 1])
    assert_nil vector_set.attributes(1)

    assert_equal true, vector_set.update_attributes(1, {"category" => "A"})
    assert_equal ({"category" => "A"}), vector_set.attributes(1)

    assert_equal true, vector_set.update_attributes(1, {"quantity" => 2, "size" => 1.5})
    assert_equal ({"quantity" => 2, "size" => 1.5}), vector_set.attributes(1)

    assert_equal true, vector_set.update_attributes(1, {})
    assert_empty vector_set.attributes(1)
  end

  def test_remove_attributes
    vector_set.add(1, [1, 1, 1], attributes: {"category" => "A"})
    assert_equal ({"category" => "A"}), vector_set.attributes(1)

    assert_equal true, vector_set.remove_attributes(1)
    assert_nil vector_set.attributes(1)

    assert_equal false, vector_set.remove_attributes(2)
  end

  def test_nearest
    add_items(vector_set)
    result = vector_set.nearest(1)
    assert_equal [3, 2], result.map { |v| v[:id] }
    assert_elements_in_delta [0.9082482755184174, 0], result.map { |v| v[:score] }
  end

  def test_nearest_attributes
    vector_set.add(1, [1, 1, 1], attributes: {category: "A"})
    vector_set.add(2, [-1, -1, -1], attributes: {category: "B"})
    vector_set.add(3, [1, 1, 0])

    result = vector_set.nearest(1, with_attributes: true)
    assert_empty result[0][:attributes]
    assert_equal ({"category" => "B"}), result[1][:attributes]
  end

  def test_nearest_exact
    add_items(vector_set)
    result = vector_set.nearest(1, exact: true)
    assert_equal [3, 2], result.map { |v| v[:id] }
    assert_elements_in_delta [0.9082482755184174, 0], result.map { |v| v[:score] }
  end

  def test_nearest_missing
    add_items(vector_set)
    error = assert_raises do
      vector_set.nearest(4)
    end
    assert_match "element not found in set", error.message
  end

  def test_search
    add_items(vector_set)
    result = vector_set.search([1, 1, 1])
    assert_equal [1, 3, 2], result.map { |v| v[:id] }
    assert_elements_in_delta [1, 0.9082482755184174, 0], result.map { |v| v[:score] }
  end

  def test_search_attributes
    vector_set.add(1, [1, 1, 1], attributes: {category: "A"})
    vector_set.add(2, [-1, -1, -1], attributes: {category: "B"})
    vector_set.add(3, [1, 1, 0])

    result = vector_set.search([1, 1, 1], with_attributes: true)
    assert_equal ({"category" => "A"}), result[0][:attributes]
    assert_empty result[1][:attributes]
    assert_equal ({"category" => "B"}), result[2][:attributes]
  end

  def test_search_ef
    add_items(vector_set)
    result = vector_set.search([1, 1, 1], ef: 2)
    # still returns 3 results
    assert_equal [1, 3, 2], result.map { |v| v[:id] }
    assert_elements_in_delta [1, 0.9082482755184174, 0], result.map { |v| v[:score] }
  end

  def test_search_exact
    add_items(vector_set)
    result = vector_set.search([1, 1, 1], exact: true)
    assert_equal [1, 3, 2], result.map { |v| v[:id] }
    assert_elements_in_delta [1, 0.9082482755184174, 0], result.map { |v| v[:score] }
  end

  def test_search_different_dimensions
    add_items(vector_set)
    error = assert_raises do
      vector_set.search([1, 1])
    end
    assert_match "Vector dimension mismatch - got 2 but set has 3", error.message
  end

  def test_links
    add_items(vector_set)
    links = vector_set.links(1)
    assert_equal [2, 3], links.last.map { |v| v[:id] }
    assert_elements_in_delta [0, 0.9082482755184174], links.last.map { |v| v[:score] }

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
    result = vector_set.nearest(1)
    assert_equal [3], result.map { |v| v[:id] }
    assert_elements_in_delta [0.9082482755184174], result.map { |v| v[:score] }
  end

  def test_id_type_integer
    vector_set = Neighbor::Redis::VectorSet.new("items", id_type: "integer")
    vector_set.add(1, [1, 1, 1])
    vector_set.add("2", [-1, -1, -1])
    error = assert_raises(ArgumentError) do
      vector_set.add("3a", [1, 1, 0])
    end
    assert_match "invalid value for Integer()", error.message
    assert_equal [2], vector_set.nearest(1).map { |v| v[:id] }
    assert_equal [1, 2], vector_set.search([1, 1, 1]).map { |v| v[:id] }
    assert_equal [2], vector_set.links(1).last.map { |v| v[:id] }
    assert_equal [1, 2], vector_set.sample(2).sort
  end

  def test_id_type_string
    vector_set = Neighbor::Redis::VectorSet.new("items", id_type: "string")
    vector_set.add(1, [1, 1, 1])
    vector_set.add("2", [-1, -1, -1])
    assert_equal ["2"], vector_set.nearest(1).map { |v| v[:id] }
    assert_equal ["1", "2"], vector_set.search([1, 1, 1]).map { |v| v[:id] }
    assert_equal ["2"], vector_set.links(1).last.map { |v| v[:id] }
    assert_equal ["1", "2"], vector_set.sample(2).sort
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
      [1, 1, 0]
    ]
    vector_set.add_all(ids, vectors)
  end
end
