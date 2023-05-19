package com.hmdp.utils;

import cn.hutool.core.bean.BeanUtil;
import com.hmdp.dto.UserDTO;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.web.servlet.HandlerInterceptor;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * @author xck
 * @date 2022/11/28 15:22
 */

/**
 * 登录拦截器
 */
public class LoginInterceptor  implements HandlerInterceptor {


    @Override
    public boolean preHandle(HttpServletRequest request, HttpServletResponse response, Object handler) throws Exception {
        //没有登录用户，拦截
        if (UserHolder.getUser() == null) {
            response.setStatus(401);
            return false;
        }
        //登录过，放行
        return true;
    }
}
