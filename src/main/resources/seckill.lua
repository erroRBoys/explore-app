--1.定义动态变量
local voucherId = ARGV[1]
local userId = ARGV[2]
local orderId = ARGV[2]

local stockKey = "seckill:stock:" .. voucherId
local orderKey = "seckill:order:" .. voucherId
--2.判定是否能够下单
if (tonumber(redis.call("get", stockKey)) <= 0) then
    --2.1库存不足
    return 1
end

if (redis.call("sismember", orderKey, userId) == 1) then
    --2.2购买过
    return 2
end

--3.能够下单
redis.call("incrby", stockKey, -1)
redis.call("sadd", orderKey, userId)
--4.发送订单信息到消息队列
redis.call('xadd', 'stream.orders', '*', 'userId', userId, 'voucherId', voucherId, 'id', orderId)
return 0
