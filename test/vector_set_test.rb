require_relative "test_helper"

class VectorSetTest < Minitest::Test
  def setup
    skip if server_version.to_i < 8
    super
    vector_set.drop if vector_set.exists?
  end

  def test_nearest_by_id
    add_items(vector_set)
    result = vector_set.nearest_by_id(1)
    assert_equal [3, 2], result.map { |v| v[:id].to_i }
    assert_elements_in_delta [0.9082482755184174, 0], result.map { |v| v[:score] }
  end

  def test_nearest_by_vector
    add_items(vector_set)
    result = vector_set.nearest_by_vector([1, 1, 1])
    assert_equal [1, 3, 2], result.map { |v| v[:id].to_i }
    assert_elements_in_delta [1, 0.9082482755184174, 0], result.map { |v| v[:score] }
  end

  def test_find
    add_items(vector_set)
    assert_elements_in_delta [1, 1, 1], vector_set.find(1)
    assert_elements_in_delta [-1, -1, -1], vector_set.find(2)
    assert_elements_in_delta [1, 1, 0], vector_set.find(3)
    assert_nil vector_set.find(4)
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

  def test_count
    add_items(vector_set)
    assert_equal 3, vector_set.count
  end

  def test_nearest_by_id_attributes
    vector_set.add(1, [1, 1, 1], attributes: {category: "A"})
    vector_set.add(2, [-1, -1, -1], attributes: {category: "B"})
    vector_set.add(3, [1, 1, 0])

    result = vector_set.nearest_by_id(1, with_attributes: true)
    assert_empty result[0][:attributes]
    assert_equal "B", result[1][:attributes]["category"]
  end

  def test_nearest_by_vector_attributes
    vector_set.add(1, [1, 1, 1], attributes: {category: "A"})
    vector_set.add(2, [-1, -1, -1], attributes: {category: "B"})
    vector_set.add(3, [1, 1, 0])

    result = vector_set.nearest_by_vector([1, 1, 1], with_attributes: true)
    assert_equal "A", result[0][:attributes]["category"]
    assert_empty result[1][:attributes]
    assert_equal "B", result[2][:attributes]["category"]
  end

  private

  def vector_set
    @vector_set ||= Neighbor::Redis::VectorSet.new("items", dimensions: 3)
  end

  def add_items(vector_set)
    vector_set.add(1, [1, 1, 1])
    vector_set.add(2, [-1, -1, -1])
    vector_set.add(3, [1, 1, 0])
  end
end
