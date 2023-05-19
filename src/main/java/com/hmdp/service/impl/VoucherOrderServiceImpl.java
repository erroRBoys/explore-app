package com.hmdp.service.impl;

import cn.hutool.core.bean.BeanUtil;
import com.hmdp.dto.Result;
import com.hmdp.entity.VoucherOrder;
import com.hmdp.mapper.VoucherOrderMapper;
import com.hmdp.service.ISeckillVoucherService;
import com.hmdp.service.IVoucherOrderService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.utils.RedisIdWorker;
import com.hmdp.utils.SimpleRedisLock;
import com.hmdp.utils.UserHolder;
import lombok.extern.slf4j.Slf4j;
import org.springframework.aop.framework.AopContext;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.redis.connection.stream.*;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import javax.annotation.PostConstruct;
import javax.annotation.Resource;
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * <p>
 * 服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Slf4j
@Service
public class VoucherOrderServiceImpl extends ServiceImpl<VoucherOrderMapper, VoucherOrder> implements IVoucherOrderService {

    @Resource
    ISeckillVoucherService seckillVoucherService;
    @Resource
    RedisIdWorker redisIdWorker;
    @Resource
    StringRedisTemplate stringRedisTemplate;

    private static final DefaultRedisScript<Long> SECKILL_SCRIPT;
    private IVoucherOrderService orderService;

    static {
        SECKILL_SCRIPT = new DefaultRedisScript<>();
        SECKILL_SCRIPT.setLocation(new ClassPathResource("seckill.lua"));
        SECKILL_SCRIPT.setResultType(Long.class);
    }


    private static final ExecutorService es = Executors.newSingleThreadExecutor();

    @PostConstruct
    private void init(){
        es.submit((Runnable) () -> {
            while (true){
                try {
                    // 1.获取消息队列中的订单信息 XREADGROUP GROUP g1 c1 COUNT 1 BLOCK 2000 STREAMS s1 >
                    List<MapRecord<String, Object, Object>> list = stringRedisTemplate.opsForStream().read(
                            Consumer.from("g1", "c1"),
                            StreamReadOptions.empty().count(1).block(Duration.ofSeconds(2)),
                            StreamOffset.create("stream.orders", ReadOffset.lastConsumed())
                    );
                    // 2.判断订单信息是否为空
                    if (list == null || list.isEmpty()) {
                        // 如果为null，说明没有消息，继续下一次循环
                        continue;
                    }
                    // 解析数据
                    MapRecord<String, Object, Object> record = list.get(0);
                    Map<Object, Object> value = record.getValue();
                    VoucherOrder voucherOrder = BeanUtil.fillBeanWithMap(value, new VoucherOrder(), true);
                    //处理订单（创建订单）
                    handlerVoucherOrder(voucherOrder);
                    // 确认消息 XACK
                    stringRedisTemplate.opsForStream().acknowledge("s1", "g1", record.getId());
                } catch (Exception e) {
                    log.error("处理订单异常", e);
                    //处理异常消息
                    handlePendingList();
                }
            }
        });
    }

    private void handlePendingList() {
        while (true) {
            try {
                // 1.获取pending-list中的订单信息 XREADGROUP GROUP g1 c1 COUNT 1 BLOCK 2000 STREAMS s1 0
                List<MapRecord<String, Object, Object>> list = stringRedisTemplate.opsForStream().read(
                        Consumer.from("g1", "c1"),
                        StreamReadOptions.empty().count(1),
                        StreamOffset.create("stream.orders", ReadOffset.from("0"))
                );
                // 2.判断订单信息是否为空
                if (list == null || list.isEmpty()) {
                    // 如果为null，说明没有异常消息，结束循环
                    break;
                }
                // 解析数据
                MapRecord<String, Object, Object> record = list.get(0);
                Map<Object, Object> value = record.getValue();
                VoucherOrder voucherOrder = BeanUtil.fillBeanWithMap(value, new VoucherOrder(), true);
                // 3.创建订单
                createVoucherOrder(voucherOrder);
                // 4.确认消息 XACK
                stringRedisTemplate.opsForStream().acknowledge("s1", "g1", record.getId());
            } catch (Exception e) {
                log.error("处理pendding订单异常", e);
                try {
                    Thread.sleep(20);
                } catch (Exception te) {
                    te.printStackTrace();
                }
            }
        }
    }

    @Override
    public Result seckillVoucher(Long voucherId) {

        Long userId = UserHolder.getUser().getId();
        long orderId = redisIdWorker.nextId("order");
        Long res = stringRedisTemplate.execute(
                SECKILL_SCRIPT,
                Collections.emptyList(),
                voucherId.toString(),
                userId.toString(),
                String.valueOf(orderId)
        );

        if(res.intValue() != 0){
            return Result.fail(res.intValue() == 1 ? "库存不足" : "该优惠券不可重复购买");
        }

        orderService = (IVoucherOrderService) AopContext.currentProxy();
        return Result.ok(orderId);

    }

    //阻塞队列，存储订单信息
    /*private BlockingQueue<VoucherOrder> blockingQueue = new ArrayBlockingQueue<>(1024 * 1024);
    @Override
    public Result seckillVoucher(Long voucherId) {

        Long userId = UserHolder.getUser().getId();
        long orderId = redisIdWorker.nextId("order");
        Long res = stringRedisTemplate.execute(
                SECKILL_SCRIPT,
                Collections.emptyList(),
                voucherId.toString(),
                userId.toString());

        if(res.intValue() != 0){
            return Result.fail(res.intValue() == 1 ? "库存不足" : "该优惠券不可重复购买");
        }

        VoucherOrder voucherOrder = new VoucherOrder();
        voucherOrder.setUserId(userId);
        voucherOrder.setVoucherId(voucherId);
        voucherOrder.setId(orderId);
        // todo 在阻塞队列中进行数据库操作
        blockingQueue.add(voucherOrder);
        orderService = (IVoucherOrderService) AopContext.currentProxy();
        return Result.ok(orderId);

    }*/

    private void handlerVoucherOrder(VoucherOrder voucherOrder){
        Long userId = voucherOrder.getUserId();
        //尝试获取锁
        SimpleRedisLock redisLock = new SimpleRedisLock("shop" + userId, stringRedisTemplate);
        boolean success = redisLock.tryLook(1200);
        if (!success) {
            log.error("不可重复购买该优惠券");
            return;
        }
        try {
            orderService.createVoucherOrder(voucherOrder);
        } catch (IllegalStateException e) {
            throw new RuntimeException(e);
        } finally {
            redisLock.unlock();
        }
    }

    /*@Override
    public Result seckillVoucher(Long voucherId) {

        SeckillVoucher byId = seckillVoucherService.getById(voucherId);
        //判断时间
        if (byId.getBeginTime().isAfter(LocalDateTime.now())) {
            return Result.fail("秒杀未开始!");
        }
        if (byId.getEndTime().isBefore(LocalDateTime.now())) {
            return Result.fail("秒杀已结束！");
        }
        //判断库存
        if (byId.getStock() < 1) {
            return Result.fail("券已售完！");
        }
        Long userId = UserHolder.getUser().getId();

        //尝试获取锁
        SimpleRedisLock redisLock = new SimpleRedisLock("shop" + userId, stringRedisTemplate);
        boolean success = redisLock.tryLook(1200);
        if (!success) {
            return Result.fail("您已购买过该优惠券！");
        }

        try {
            IVoucherOrderService orderService = (IVoucherOrderService) AopContext.currentProxy();
            return orderService.getResult(voucherId);
        } catch (IllegalStateException e) {
            throw new RuntimeException(e);
        } finally {
            redisLock.unlock();
        }

    }*/

    @Transactional
    public void createVoucherOrder(VoucherOrder voucherOrder) {
        Long userId = voucherOrder.getUserId();
        int orderNum = query().eq("user_id", userId).eq("voucher_id", voucherOrder.getVoucherId()).count();
        //一人一单判断
        if (orderNum > 0) {
            log.error("不可重复购买该优惠券");
            return;
        }
        //更新库存
        boolean success = seckillVoucherService.update()
                .setSql("stock = stock -1")
                .eq("voucher_id", voucherOrder.getVoucherId())
                .gt("stock", 0).update();
        if (!success) {
            log.error("购买失败");
            return;
        }
        //生成订单
        save(voucherOrder);
    }
}
