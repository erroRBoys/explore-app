package com.hmdp.dto;

/**
 * @author xck
 * @date 2022/12/1 15:23
 */

import lombok.Data;
import java.time.LocalDateTime;

/**
 * 逻辑过期类
 */
@Data
public class RedisData {
    //过期时间
    public LocalDateTime expire;
    public Object data;
}
