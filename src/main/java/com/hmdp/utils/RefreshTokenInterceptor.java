package com.hmdp.utils;

import cn.hutool.core.bean.BeanUtil;
import cn.hutool.core.util.StrUtil;
import com.hmdp.dto.UserDTO;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.web.servlet.HandlerInterceptor;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import static com.hmdp.utils.RedisConstants.LOGIN_USER_KEY;

/**
 * @author xck
 * @date 2022/11/28 15:22
 */
public class RefreshTokenInterceptor  implements HandlerInterceptor {

    private StringRedisTemplate stringRedisTemplate;

    public RefreshTokenInterceptor(StringRedisTemplate stringRedisTemplate){
        this.stringRedisTemplate = stringRedisTemplate;
    }

    @Override
    public boolean preHandle(HttpServletRequest request, HttpServletResponse response, Object handler) throws Exception {

        //获取token
        String token = request.getHeader("authorization");
        if (StrUtil.isBlank(token) ) {
            return true;
        }
        //从redis获取对象
        Map<Object, Object> userMap = stringRedisTemplate.opsForHash().entries(LOGIN_USER_KEY+token);
        UserDTO userDTO = BeanUtil.fillBeanWithMap(userMap, new UserDTO(), false);
        if (userDTO == null) {
            response.setStatus(401);
            return true;
        }
        UserHolder.saveUser(userDTO);
        //每次访问更新redis-token过期时间
        stringRedisTemplate.expire(LOGIN_USER_KEY+token,30, TimeUnit.DAYS);
        return true;
    }
    @Override
    public void afterCompletion(HttpServletRequest request, HttpServletResponse response, Object handler, Exception ex) throws Exception {
        // 移除用户
        UserHolder.removeUser();
    }
}
