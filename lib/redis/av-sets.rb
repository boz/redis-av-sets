require 'redis'
class Redis
class AVSets
  attr_reader(:redis,:prefix)

  def initialize(redis = Redis.new, prefix = "redis-av-sets")
    @redis, @prefix = redis, prefix
  end

  def size
    redis.hlen(id_lookup_key).to_i
  end

  def store!(avset)
    id , created = identify!(avset)
    register!(id,avset) if created
    id
  end

  def get(id)
    if encoded = redis.hget(object_lookup_key,id)
      decode(encoded)
    end
  end

  # todo: store all keys in a set. Redis#keys is slow.
  def clear!
    redis.keys("#{prefix}:*").each do |key|
      redis.del(key)
    end
  end

  def supersets(avset)
    superset_attributes(avset).inject({}) do |acc,key|
      acc.merge(key => superset_values(avset,key))
    end
  end

  def superset_attributes(avset)
    sets = redis.sinter(*superset_sets(avset)).map do |id|
      attributes_by_id_key(id)
    end
    (redis.sunion(*sets) - avset.keys.map(&:to_s)).to_set
  end

  def superset_values(avset, attribute)
    sets = superset_sets(avset)
    sets.push(ids_by_attribute_key(attribute))
    sets = redis.sinter(*sets).map do |id|
      value_by_id_key(id,attribute)
    end
    return Set.new if sets.empty?
    redis.sunion(*sets).to_set
  end

  protected
  def superset_sets(avset = {})
    sets = [all_ids_key]
    avset.each do |name,value|
      sets.push(ids_by_attribute_value_key(name,value))
    end
    sets
  end

  def identify!(avset)
    encoded = encode(avset)

    id      = redis.hget(id_lookup_key,encoded)
    return [id.to_i, false] if id

    id = redis.incr(id_counter_key).to_i
    ok = redis.hsetnx(id_lookup_key,encoded,id)

    if !ok
      id = redis.hget(id_lookup_key,encoded)
      return [id, false]
    end

    redis.hset(object_lookup_key,id,encoded)

    return [id, true]
  end

  def register!(id,avset)
    redis.sadd(all_ids_key,id)
    avset.each do |name,value|
      redis.sadd(attributes_by_id_key(id),name)
      redis.sadd(value_by_id_key(id,name),value)
      redis.sadd(ids_by_attribute_key(name),id)
      redis.sadd(ids_by_attribute_value_key(name,value),id)
    end
  end

  def encode(avset)
    Marshal.dump(avset.to_a.sort_by { |x| x[0].to_s })
  end

  def decode(encoded)
    Marshal.load(encoded).inject({}) do |acc,x|
      acc.merge!(x[0] => x[1])
    end
  end

  def all_ids_key
    "#{prefix}:id:all"
  end

  def id_counter_key
    "#{prefix}:id:counter"
  end

  def id_lookup_key
    "#{prefix}:id:lookup"
  end

  def object_lookup_key
    "#{prefix}:object:lookup"
  end

  def attributes_by_id_key(id)
    "#{prefix}:names-by-id:#{id}"
  end

  def value_by_id_key(id,name)
    "#{prefix}:value-by-id:#{id}:#{name}"
  end

  def ids_by_attribute_key(name)
    "#{prefix}:ids-by-name:#{name}"
  end

  def ids_by_attribute_value_key(name,value)
    "#{prefix}:ids-by-name-value:#{name}:#{value}"
  end
end
end
