-- KEYS[1]: spatialIndexKey (e.g., spatial_index:00001787473200000)
-- ARGV[1]: centerLon,
-- ARGV[2]: centerLat,
-- ARGV[3]: widthKm,
-- ARGV[4]: heightKm

local cellKeys = redis.call('GEOSEARCH', KEYS[1], 'FROMLONLAT', ARGV[1], ARGV[2], 'BYBOX', ARGV[3], ARGV[4], 'km')

if #cellKeys == 0 then
    return {}
end
local payloads = redis.call('MGET', unpack(cellKeys))
local result = {}
for i = 1, #cellKeys do
    table.insert(result, cellKeys[i])
    table.insert(result, payloads[i])
end

return result