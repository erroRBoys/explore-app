package com.hmdp.utils;

import cn.hutool.Hutool;
import cn.hutool.core.lang.UUID;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;

import java.util.Collections;
import java.util.concurrent.TimeUnit;

/**
 * @author xck
 * @date 2022/12/9 16:50
 */
public class SimpleRedisLock implements ILock{

    //
    String name;
    //
    StringRedisTemplate stringRedisTemplate;

    public SimpleRedisLock(String name, StringRedisTemplate stringRedisTemplate) {
        this.name = name;
        this.stringRedisTemplate = stringRedisTemplate;
    }

    private static final String prefix = "lock:";
    private static final String id_preifx = UUID.randomUUID().toString(true);

    private static final DefaultRedisScript<Long> UNLOCK_SCRIPT;
    static {
        UNLOCK_SCRIPT = new DefaultRedisScript<>();
        UNLOCK_SCRIPT.setLocation(new ClassPathResource("unlock.lua"));
        UNLOCK_SCRIPT.setResultType(Long.class);
    }

    /**
     * 尝试获取锁
     * @param timeoutSec
     * @return
     */
    @Override
    public boolean tryLook(long timeoutSec) {

        String threadId = id_preifx + Thread.currentThread().getId();
        Boolean success = stringRedisTemplate.opsForValue()
                .setIfAbsent(prefix + name,  threadId, timeoutSec, TimeUnit.SECONDS);

        return Boolean.TRUE.equals(success);
    }

    /**
     * 释放锁
     */
    @Override
    public void unlock() {
        stringRedisTemplate.execute(UNLOCK_SCRIPT,
                Collections.singletonList(prefix + name),
                id_preifx + Thread.currentThread().getId());
    }
    //    @Override
//    public void unlock() {
//        String threadId = id_preifx + Thread.currentThread().getId();
//        String dbId = stringRedisTemplate.opsForValue().get(prefix + name);
//        if(threadId.equals(dbId)){
//            stringRedisTemplate.delete(prefix + name);
//        }
//    }
}
