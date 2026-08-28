-- KEYS[1]: individual_cell_key (e.g., weather:map:00001787473200000:55.0:-61.0)
-- ARGV[1]: lat
-- ARGV[2]: lon
-- ARGV[3]: serializedLayers (binary blob)
-- ARGV[4]: ttlInSeconds
-- ARGV[5]: timestamp (e.g., 00001787473200000)

local ttlInSeconds = tonumber(ARGV[4])
local index_key = 'spatial_index:' .. ARGV[5]

redis.call('SET', KEYS[1], ARGV[3])
redis.call('EXPIRE', KEYS[1], ttlInSeconds)
redis.call('GEOADD', index_key, ARGV[2], ARGV[1], KEYS[1])
redis.call('EXPIRE', index_key, ttlInSeconds)

return "SUCCESS"