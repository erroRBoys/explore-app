package com.hmdp.service.impl;

import cn.hutool.core.bean.BeanUtil;
import cn.hutool.core.bean.copier.CopyOptions;
import cn.hutool.core.lang.UUID;
import cn.hutool.core.util.RandomUtil;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.dto.LoginFormDTO;
import com.hmdp.dto.Result;
import com.hmdp.dto.UserDTO;
import com.hmdp.entity.User;
import com.hmdp.mapper.UserMapper;
import com.hmdp.service.IUserService;
import com.hmdp.utils.RegexUtils;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import javax.servlet.http.HttpSession;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static com.hmdp.utils.RedisConstants.*;
import static com.hmdp.utils.SystemConstants.USER_NICK_NAME_PREFIX;

/**
 * <p>
 * 服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Service
public class UserServiceImpl extends ServiceImpl<UserMapper, User> implements IUserService {

    @Resource
    private StringRedisTemplate stringRedisTemplate;
    @Override
    public Result sendCode(String phone, HttpSession session) {

        //验证手机号
        if (RegexUtils.isPhoneInvalid(phone)) { //有误
            return Result.fail("手机号格式错误！");
        }
        //生成验证码
        String code = RandomUtil.randomNumbers(6);
        //将验证码储存进入Redis，并设置过期时间
        stringRedisTemplate.opsForValue().set(LOGIN_CODE_KEY+phone,code,2, TimeUnit.MINUTES);

        //发送验证码
        log.debug("成功发送验证码 " + code);
        return Result.ok();
    }

    @Override
    public Result login(LoginFormDTO loginForm, HttpSession session) {

        //验证手机号
        String phone = loginForm.getPhone();
        if(RegexUtils.isPhoneInvalid(phone)){
            return Result.fail("手机号错误！");
        }
        //判断验证码
        String code = loginForm.getCode();
        if (code == null || !code.equals(stringRedisTemplate.opsForValue().get(LOGIN_CODE_KEY+phone))) {
            return Result.fail("验证码有误！");
        }
        User user = query().eq("phone", phone).one();

        if(user == null){
            user = createUser(phone);
        }

        //生成token
        String token = UUID.randomUUID().toString(true);

        //user存入redis
        UserDTO userDTO = BeanUtil.copyProperties(user, UserDTO.class);
        Map<String, Object> userMap = BeanUtil.beanToMap(userDTO, new HashMap<>(), CopyOptions.create()
                .setIgnoreNullValue(true)
                .setFieldValueEditor((key, value) -> value.toString()));
        stringRedisTemplate.opsForHash().putAll(LOGIN_USER_KEY+token,userMap);
        //设置过期时间
        stringRedisTemplate.expire(LOGIN_USER_KEY+token,30,TimeUnit.MINUTES);
        //返回token给前端
        return Result.ok(token);
    }

    private User createUser(String phone) {
        User user = new User();
        user.setPhone(phone);
        user.setNickName(USER_NICK_NAME_PREFIX+RandomUtil.randomString(10));
        save(user);
        return user;
    }
}
