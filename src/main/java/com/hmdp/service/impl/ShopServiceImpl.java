package com.hmdp.service.impl;

import cn.hutool.core.util.BooleanUtil;
import cn.hutool.core.util.StrUtil;
import cn.hutool.json.JSONObject;
import cn.hutool.json.JSONUtil;
import com.baomidou.mybatisplus.extension.plugins.pagination.Page;
import com.hmdp.dto.RedisData;
import com.hmdp.dto.Result;
import com.hmdp.entity.Shop;
import com.hmdp.mapper.ShopMapper;
import com.hmdp.service.IShopService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.utils.CacheClient;
import com.hmdp.utils.SystemConstants;
import org.springframework.data.geo.Distance;
import org.springframework.data.geo.GeoResult;
import org.springframework.data.geo.GeoResults;
import org.springframework.data.redis.connection.RedisGeoCommands;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.domain.geo.GeoReference;
import org.springframework.stereotype.Service;
import javax.annotation.Resource;
import java.time.LocalDateTime;
import java.util.*;
import java.util.concurrent.*;

import static com.hmdp.utils.RedisConstants.*;

/**
 * <p>
 *  服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Service
public class ShopServiceImpl extends ServiceImpl<ShopMapper, Shop> implements IShopService {

    @Resource
    StringRedisTemplate stringRedisTemplate;
    @Resource
    CacheClient cacheClient;

    @Override
    public Result queryById(Long id) {
        //缓存穿透
//        Shop shop = cacheClient
//                .cacheThrough(CACHE_SHOP_KEY, id, Shop.class, this::getById, CACHE_SHOP_TTL, TimeUnit.SECONDS);

        //互斥锁防止缓存击穿
//        Shop shop = queryWithMutex(id);

        //逻辑过期防止缓存击穿
        Shop shop = cacheClient.logicExpire(CACHE_SHOP_KEY, id, Shop.class, this::getById, CACHE_SHOP_TTL, TimeUnit.SECONDS);
        if (shop == null) {
            Result.fail("商铺不存在！");
        }
        System.out.println("commit1");
        return Result.ok(shop);
    }

    private static final ExecutorService CACHE_REBUILD_EXECUTOR = Executors.newFixedThreadPool(10);

    //逻辑过期解决缓存击穿
    public Shop logicExpire(Long id){
        //查redis
        String shopJson = stringRedisTemplate.opsForValue().get(CACHE_SHOP_KEY+id);
        //判断数据是否逻辑过期
        RedisData redisData = JSONUtil.toBean(shopJson, RedisData.class);
        Shop shop = JSONUtil.toBean((JSONObject) redisData.getData(), Shop.class);
        //没有过期，直接返回
        if(redisData.getExpire().isAfter(LocalDateTime.now())){
            return shop;
        }
        //过期，缓存重建
        boolean lock = tryLock(id);
        //是否获取到锁
        //获取到了，开启新线程，进行数据重建
        if(lock){
            //需要doublecheck缓存
            //查redis
            shopJson = stringRedisTemplate.opsForValue().get(CACHE_SHOP_KEY+id);
            //判断数据是否逻辑过期
            redisData = JSONUtil.toBean(shopJson, RedisData.class);
            shop = JSONUtil.toBean((JSONObject) redisData.getData(), Shop.class);
            //没有过期，直接返回
            if(redisData.getExpire().isAfter(LocalDateTime.now())){
                return shop;
            }
            CACHE_REBUILD_EXECUTOR.submit(() -> {
                try {
                    rebuilderCache(id,5L);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                } finally {
                    unLock(id);
                }
            });
        }
        //没有，返回数据
        return shop;
    }

    //互斥锁解决缓存击穿
    public Shop queryWithMutex(Long id){
        //查redis
        String shopJson = stringRedisTemplate.opsForValue().get(CACHE_SHOP_KEY+id);
        //有数据直接返回
        if (StrUtil.isNotBlank(shopJson)) {
            Shop shop = JSONUtil.toBean(shopJson, Shop.class);
            return shop;
        }
        if (shopJson != null) {
            return null;
        }
        //缓存未命中
        //没有获取到锁，休眠重试
        Shop byId = null;
        try {
            if (!tryLock(id)) {
                Thread.sleep(50);
                return queryWithMutex(id);
            }
            //获取到锁需要再看缓存是否存在（doubleCheck）， 如果缓存存在不用重建缓存
            String doubleCache = stringRedisTemplate.opsForValue().get("cache:shop:"+id);
            if (StrUtil.isNotBlank(doubleCache)) {
                Shop shop = JSONUtil.toBean(doubleCache, Shop.class);
                return shop;
            }
            //没有 查数据库
            byId = getById(id);
            //模拟重建延时
            Thread.sleep(200);
            //没有数据 报错
            if (byId == null) {
                //存入null值，防止缓存穿透
                stringRedisTemplate.opsForValue().set("cache:shop:"+id,"",2L, TimeUnit.MINUTES);
                return null;
            }
            String shop = JSONUtil.toJsonStr(byId);
            //有数据 存入redis
            //设置过期时间，一定程度上可以保证数据一致性
            stringRedisTemplate.opsForValue().set("cache:shop:"+id,shop,30L, TimeUnit.MINUTES);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        } finally {
            unLock(id);
        }
        //返回数据
        return byId;
    }

    //添加互斥锁
    public boolean tryLock(Long id){
        Boolean aBoolean = stringRedisTemplate.opsForValue().setIfAbsent("lock:shop:" + id, "1", 10, TimeUnit.SECONDS);
        return BooleanUtil.isTrue(aBoolean);
    }
    //删除互斥锁
    public void unLock(Long id){
        stringRedisTemplate.delete("lock:shop:"+id);
    }

    //逻辑过期缓存重建
    public void rebuilderCache(Long id, Long expireTime) throws InterruptedException {
        RedisData rd = new RedisData();
        Shop shop = getById(id);
        //模拟缓存重建时的复杂逻辑耗时
        Thread.sleep(200);
        rd.setData(shop);
        rd.setExpire(LocalDateTime.now().plusSeconds(expireTime));
        stringRedisTemplate.opsForValue().set(CACHE_SHOP_KEY+id,JSONUtil.toJsonStr(rd));
    }

    //缓存穿透
    public Shop cacheThrough(Long id){
        //查redis
        String shopJson = stringRedisTemplate.opsForValue().get("cache:shop:"+id);
        //有数据直接返回
        if (StrUtil.isNotBlank(shopJson)) {
            Shop shop = JSONUtil.toBean(shopJson, Shop.class);
            return shop;
        }
        //没有数据防止缓存穿透
        if (shopJson != null) { //不为空，并且不为null，则是防止缓存穿透的 "" 空字符缓存
            return null;
        }
        //没有 查数据库
        Shop byId = getById(id);
        //没有数据 报错
        if (byId == null) {
            //存入null值，防止缓存穿透
            stringRedisTemplate.opsForValue().set("cache:shop:"+id,"",2L, TimeUnit.MINUTES);
            return null;
        }
        String shop = JSONUtil.toJsonStr(byId);
        //有数据 存入redis
        //设置过期时间，一定程度上可以保证数据一致性
        stringRedisTemplate.opsForValue().set("cache:shop:"+id,shop,30L, TimeUnit.MINUTES);
        //返回数据
        return byId;
    }

    @Override
    public Result updateShop(Shop shop) {

        //更新数据库
        updateById(shop);
        //删除redis缓存
        stringRedisTemplate.delete("cache:shop:"+shop.getId());
        return Result.ok();
    }

    @Override
    public Result queryShopByType(Integer typeId, Integer current, Double x, Double y) {
        // 1.判断是否需要根据坐标查询
        if (x == null || y == null) {
            // 不需要坐标查询，按数据库查询
            Page<Shop> page = query()
                    .eq("type_id", typeId)
                    .page(new Page<>(current, SystemConstants.DEFAULT_PAGE_SIZE));
            // 返回数据
            return Result.ok(page.getRecords());
        }

        // 2.计算分页参数
        int from = (current - 1) * SystemConstants.DEFAULT_PAGE_SIZE;
        int end = current * SystemConstants.DEFAULT_PAGE_SIZE;

        // 3.查询redis、按照距离排序、分页。结果：shopId、distance
        String key = SHOP_GEO_KEY + typeId;
        GeoResults<RedisGeoCommands.GeoLocation<String>> results = stringRedisTemplate.opsForGeo() // GEOSEARCH key BYLONLAT x y BYRADIUS 10 WITHDISTANCE
                .search(
                        key,
                        GeoReference.fromCoordinate(x, y),
                        new Distance(5000),
                        RedisGeoCommands.GeoSearchCommandArgs.newGeoSearchArgs().includeDistance().limit(end)
                );
        // 4.解析出id
        if (results == null) {
            return Result.ok(Collections.emptyList());
        }
        List<GeoResult<RedisGeoCommands.GeoLocation<String>>> list = results.getContent();
        if (list.size() <= from) {
            // 没有下一页了，结束
            return Result.ok(Collections.emptyList());
        }
        // 4.1.截取 from ~ end的部分
        List<Long> ids = new ArrayList<>(list.size());
        Map<String, Distance> distanceMap = new HashMap<>(list.size());
        list.stream().skip(from).forEach(result -> {
            // 4.2.获取店铺id
            String shopIdStr = result.getContent().getName();
            ids.add(Long.valueOf(shopIdStr));
            // 4.3.获取距离
            Distance distance = result.getDistance();
            distanceMap.put(shopIdStr, distance);
        });
        // 5.根据id查询Shop
        String idStr = StrUtil.join(",", ids);
        List<Shop> shops = query().in("id", ids).last("ORDER BY FIELD(id," + idStr + ")").list();
        for (Shop shop : shops) {
            shop.setDistance(distanceMap.get(shop.getId().toString()).getValue());
        }
        // 6.返回
        return Result.ok(shops);
    }
}
