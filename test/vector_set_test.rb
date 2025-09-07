require_relative "test_helper"

class VectorSetTest < Minitest::Test
  def setup
    skip if server_version.to_i < 8

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
