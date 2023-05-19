package com.hmdp.utils;

import cn.hutool.core.util.BooleanUtil;
import cn.hutool.core.util.StrUtil;
import cn.hutool.json.JSONObject;
import cn.hutool.json.JSONUtil;
import com.hmdp.dto.RedisData;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;
import java.time.LocalDateTime;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import static com.hmdp.utils.RedisConstants.LOCK_SHOP_KEY;


/**
 * 封装缓存穿透和缓存击穿 工具类
 * @author xck
 * @date 2022/12/5 16:56
 */

@Component
@Slf4j
public class CacheClient {

    private static final ExecutorService CACHE_REBUILD_EXECUTOR = Executors.newFixedThreadPool(10);
    StringRedisTemplate stringRedisTemplate;

    public CacheClient(StringRedisTemplate stringRedisTemplate) {
        this.stringRedisTemplate = stringRedisTemplate;
    }

    public void set(String key, Object value, Long time, TimeUnit unit){
        stringRedisTemplate.opsForValue().set(key, JSONUtil.toJsonStr(value),time,unit);
    }

    public void setWithLogicExpire(String key, Object value, Long time, TimeUnit unit){
        RedisData redisData = new RedisData();
        redisData.setData(value);
        redisData.setExpire(LocalDateTime.now().plusSeconds(unit.toSeconds(time)));
        stringRedisTemplate.opsForValue().set(key,JSONUtil.toJsonStr(redisData));
    }

    //缓存穿透
    public <R,ID> R cacheThrough(
            String idPrefix, ID id, Class<R> type, Function<ID,R> query, Long time, TimeUnit unit){
        //查redis
        String json = stringRedisTemplate.opsForValue().get(idPrefix+id);
        //有数据直接返回
        if (StrUtil.isNotBlank(json)) {
            R r = JSONUtil.toBean(json, type);
            return r;
        }
        //没有数据防止缓存穿透
        if (json != null) { //不为空，并且不为null，则是防止缓存穿透的 "" 空字符缓存
            return null;
        }
        //没有 查数据库
        R r = query.apply(id);
        //没有数据 报错
        if (r == null) {
            //存入null值，防止缓存穿透
            stringRedisTemplate.opsForValue().set(idPrefix+id,"",2L, TimeUnit.MINUTES);
            return null;
        }
        String json2 = JSONUtil.toJsonStr(r);
        //有数据 存入redis
        //设置过期时间，一定程度上可以保证数据一致性
        stringRedisTemplate.opsForValue().set("cache:shop:"+id,json2,time, unit);
        //返回数据
        return r;
    }


    //逻辑过期实现缓存击穿
    public <R,ID> R logicExpire(String idPrefix, ID id, Class<R> type, Function<ID,R> query, Long time, TimeUnit unit){
        String key = idPrefix+id;
        //查redis
        String json = stringRedisTemplate.opsForValue().get(key);
        //判断数据是否逻辑过期
        RedisData redisData = JSONUtil.toBean(json, RedisData.class);
        R r = JSONUtil.toBean((JSONObject) redisData.getData(), type);
        //没有过期，直接返回
        if(redisData.getExpire().isAfter(LocalDateTime.now())){
            return r;
        }
        //过期，缓存重建
        boolean lock = tryLock(LOCK_SHOP_KEY+id);
        //是否获取到锁
        //获取到了，开启新线程，进行数据重建
        if(lock){
            //需要doublecheck缓存
            //查redis
            json = stringRedisTemplate.opsForValue().get(key);
            //判断数据是否逻辑过期
            redisData = JSONUtil.toBean(json, RedisData.class);
            r = JSONUtil.toBean((JSONObject) redisData.getData(), type);
            //没有过期，直接返回
            if(redisData.getExpire().isAfter(LocalDateTime.now())){
                return r;
            }
            CACHE_REBUILD_EXECUTOR.submit(() -> {
                try {
                    R apply = query.apply(id);
                    setWithLogicExpire(key, apply, time, unit);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                } finally {
                    unLock(LOCK_SHOP_KEY+id);
                }
            });
        }
        //没有，返回数据
        return r;
    }

    //添加互斥锁
    public boolean tryLock(String key){
        Boolean aBoolean = stringRedisTemplate.opsForValue().setIfAbsent(key, "1", 10, TimeUnit.SECONDS);
        return BooleanUtil.isTrue(aBoolean);
    }
    //删除互斥锁
    public void unLock(String key){
        stringRedisTemplate.delete(key);
    }
}
