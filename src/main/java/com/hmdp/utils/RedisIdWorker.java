package com.hmdp.utils;

import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;

/**
 * 全局唯一id生成器 64位： 时间戳 + redis自增id
 * @author xck
 * @date 2022/12/6 19:26
 */
@Component
public class RedisIdWorker {

    //初始时间
    private  int START_TIME_POINT = 1640995200;

    StringRedisTemplate stringRedisTemplate;

    public RedisIdWorker(StringRedisTemplate stringRedisTemplate) {
        this.stringRedisTemplate = stringRedisTemplate;
    }

    public long nextId(String prefix){

        LocalDateTime now = LocalDateTime.now();
        long curTime = now.toEpochSecond(ZoneOffset.UTC);

        //生成时间戳
        long timePoint = curTime - START_TIME_POINT;
        String curDay = now.format(DateTimeFormatter.ofPattern("yyyy:MM:dd"));

        //生成自增id
        long increment = stringRedisTemplate.opsForValue().increment("icr:" + prefix + ":" + curDay);

        return timePoint << 32 | increment;
    }

}
