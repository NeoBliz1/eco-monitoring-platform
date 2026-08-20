-- KEYS[1]: redisHistoryKey
-- ARGV[1]: geohash (binary string)
-- ARGV[2]: serializedLayers (raw binary blob)
-- ARGV[3]: ttlInHours (binary string representation of a number)

redis.call('HSET', KEYS[1], ARGV[1], ARGV[2])
local ttlInSeconds = tonumber(ARGV[3]) * 3600
redis.call('EXPIRE', KEYS[1], ttlInSeconds)

return "SUCCESS"