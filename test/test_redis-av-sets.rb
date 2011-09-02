require 'helper'
class TestRedisAvSets < Test::Unit::TestCase
  context "an avset collection" do
    setup do
      @redis      = ::Redis.new
      @collection = Redis::AVSets.new(@redis,"test-avsets-#{Time.now.to_i}")
    end

    should "have a size of 0 when empty" do
      assert_equal 0, @collection.size
    end

    context "when storing one set" do

      setup do
        @avset    = {:foo => :bar, :x => 2} 
        @avset_id = @collection.store!(@avset)
      end

      should "have a size of 1" do
        assert_equal 1, @collection.size
      end

      should "remove the avset when cleared" do
        @collection.clear!
        assert_equal 0, @collection.size
        assert_nil @collection.get(@avset_id)
      end

      should "should identify the set" do
        assert_not_nil @avset_id
      end

      should "should have a unique id for the avset" do
        assert_equal @avset_id, @collection.store!(@avset)
      end

      should "get the avset by id" do
        assert_equal @avset, @collection.get(@avset_id)
      end

      should "not find an avset that doesn't exist" do
        assert_nil @collection.get(@avset_id.succ)
      end

      should "have no superset_keys for the avset" do
        assert_equal Set.new, @collection.superset_attributes(@avset)
      end

      should "include the avset's keys in the empty avset's superset_attributes" do
        assert_equal @avset.keys.map(&:to_s).to_set, @collection.superset_attributes({})
      end

      should "have no superset_values for nonexistent attributes" do
        assert_equal Set.new, @collection.superset_values(@avset,"noexist")
      end

      should "include the avset's values in the ampty avset's superset_values" do
        assert_equal %w{bar}.to_set, @collection.superset_values({},"foo")
        assert_equal %w{2}.to_set, @collection.superset_values({},"x")
      end

      should "include the avset in the supersets for the empty set" do
        assert_equal( {"foo" => ["bar"].to_set ,
                       "x"   => ["2"  ].to_set   }, @collection.supersets({}))
      end
    end

    context "when storing avsets A and B" do
      setup do
        @avset_A    = {:a => 1, :b => 2} 
        @avset_A_id = @collection.store!(@avset_A)

        @avset_B    = {:a => 1, :c => 2} 
        @avset_B_id = @collection.store!(@avset_B)
      end

      should "have a distinct id for each avset" do
        assert_not_equal(@avset_A_id, @avset_B_id)
      end

      should "should include both sets when finding superset_attributes" do
        assert_equal %w{a b c}.to_set, @collection.superset_attributes({})
        assert_equal %w{b c}.to_set  , @collection.superset_attributes({:a => 1})
      end

      should "include both sets when finding superset_values" do
        assert_equal %w{1}.to_set    , @collection.superset_values({},:a)
        assert_equal %w{2}.to_set    , @collection.superset_values({},:b)
        assert_equal %w{2}.to_set    , @collection.superset_values({},:c)
      end

      should "include both avsets when finding supersets" do
        assert_equal({
          "a" => %w{1}.to_set,
          "b" => %w{2}.to_set,
          "c" => %w{2}.to_set,
        },@collection.supersets({}))
      end
    end

    teardown do
      @collection.clear!
    end
  end
end
