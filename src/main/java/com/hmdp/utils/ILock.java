package com.hmdp.utils;

/**
 * 集群下的一人一单
 */
public interface ILock {
    boolean tryLook(long timeoutSec);
    void unlock();
}
