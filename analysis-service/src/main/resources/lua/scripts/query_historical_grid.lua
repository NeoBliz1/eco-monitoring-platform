-- KEYS[1]: spatialIndexKey (e.g., spatial_index:00001787473200000)
-- ARGV[1]: centerLon,
-- ARGV[2]: centerLat,
-- ARGV[3]: widthKm,
-- ARGV[4]: heightKm
-- ARGV[5]: ttlInSeconds

local cellKeys = redis.call('GEOSEARCH', KEYS[1], 'FROMLONLAT', ARGV[1], ARGV[2], 'BYBOX', ARGV[3], ARGV[4], 'km')

if #cellKeys == 0 then
    return {}
end
local ttlInSeconds = tonumber(ARGV[5])
redis.call('EXPIRE', KEYS[1], ttlInSeconds)
local payloads = redis.call('MGET', unpack(cellKeys))
local result = {}
for i = 1, #cellKeys do
    redis.call('EXPIRE', cellKeys[i], ttlInSeconds)
    table.insert(result, cellKeys[i])
    table.insert(result, payloads[i])
end

return result