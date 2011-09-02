# Attribute-Value set management, backed Redis


This is not production use, and only meant for a small number of
attributes.

## example

    ruby > collection = Redis::AVSets.new
    ...
    ruby > collection.size
     => 0 
    ruby > collection.store!({:name => "a", :type => "x"})
     => 1 
    ruby > collection.store!({:name => "b", :type => "x"})
     => 2 
    ruby > collection.superset_attributes(:type => "x")
     => #<Set: {"name"}> 
    ruby > collection.superset_values({:type => "x"},"name")
     => #<Set: {"a", "b"}> 
    ruby > collection.supersets({:type => "x"})
     => {"name"=>#<Set: {"a", "b"}>} 
    ruby > collection.supersets({})
     => {"name"=>#<Set: {"a", "b"}>, "type"=>#<Set: {"x"}>} 
    ruby > collection.get(1)
     => {:type=>"x", :name=>"a"} 
    ruby > collection.get(2)
     => {:type=>"x", :name=>"b"} 
    ruby > collection.clear!
    ...
    ruby > collection.size
    => 0 

