package com.hmdp;

import cn.hutool.core.bean.BeanUtil;
import cn.hutool.core.bean.copier.CopyOptions;
import cn.hutool.core.lang.UUID;
import com.hmdp.dto.UserDTO;
import com.hmdp.entity.User;
import com.hmdp.service.IUserService;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.test.context.junit4.SpringRunner;
import javax.annotation.Resource;
import java.io.FileOutputStream;
import java.io.OutputStreamWriter;
import java.util.HashMap;
import java.util.Map;

import static com.hmdp.utils.RedisConstants.LOGIN_USER_KEY;

@RunWith(SpringRunner.class)
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@AutoConfigureMockMvc
public class UserToken {

    @Resource
    IUserService userService;
    @Resource
    StringRedisTemplate stringRedisTemplate;
    @Test
    public void getToken() throws Exception {

		//注意！这里的绝对路径设置为自己想要的地方
        OutputStreamWriter osw = new OutputStreamWriter(new FileOutputStream("C:\\Users\\stupid\\Desktop\\hmdp\\token.txt"));
		//先模拟10个用户的登录
        for (int i = 1; i < 1000; i++) {
	        /**
	    	 * 通过id从数据库中获得user对象
	    	 * 注意这里要看用户表中有没有id从10到20的用户数据
	    	 * 也就是说要生成1000个用户token，
	    	 * 你首先的有连续的从1到1000的用户id。
	     	*/
            User user = userService.getById(i);

            //生成token
            String token = UUID.randomUUID().toString(true);

            //user存入redis
            UserDTO userDTO = BeanUtil.copyProperties(user, UserDTO.class);
            Map<String, Object> userMap = BeanUtil.beanToMap(userDTO, new HashMap<>(), CopyOptions.create()
                    .setIgnoreNullValue(true)
                    .setFieldValueEditor((key, value) -> value.toString()));
            stringRedisTemplate.opsForHash().putAll(LOGIN_USER_KEY+token,userMap);
            //写入
            osw.write(token+"\n");
        }
        //关闭输出流
        osw.close();
    }
}
