package com.itheima.publisher.amqp;


import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;
import org.springframework.amqp.AmqpException;
import org.springframework.amqp.core.*;
import org.springframework.amqp.rabbit.connection.CorrelationData;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.lang.Nullable;
import org.springframework.util.concurrent.ListenableFutureCallback;

import java.util.HashMap;
import java.util.Map;

@Slf4j
@SpringBootTest
public class SpringAmqpTest {

    @Autowired
    private RabbitTemplate rabbitTemplate;


    @Test
    public void testSimpleQueue() {
        String queueName = "simple.queue";
        String message = "hello,spring amqp111";

        rabbitTemplate.convertAndSend(queueName, message);
    }

    @Test
    public void testWorkQueue() throws InterruptedException {
        String queueName = "work.queue";

        String message = "hello,message_";
        for (int i = 0; i < 50; i++) {
            //    每20ms发送一次,相当于每秒发送50条消息
            rabbitTemplate.convertAndSend(queueName, message + i);
            Thread.sleep(20);
        }
    }

    @Test
    public void testFanoutExchange() {
        String exchangeName = "hmall.fanout";
        String message = "hello,everyone";

        rabbitTemplate.convertAndSend(exchangeName, "", message);

    }

    @Test
    public void testSendDirectExchange() {
        // 交换机名称
        String exchangeName = "hmall.direct";
        // 消息
        String message = "红色警报！日本乱排核废水，导致海洋生物变异，惊现哥斯拉！";
        // 发送消息
        rabbitTemplate.convertAndSend(exchangeName, "blue", message);
    }

    /**
     * topicExchange
     */
    @Test
    public void testSendTopicExchange() {
        // 交换机名称
        String exchangeName = "hmall.topic";
        // 消息
        String message = "喜报！孙悟空大战哥斯拉，胜!";
        // 发送消息
        rabbitTemplate.convertAndSend(exchangeName, "china.news", message);
    }

    @Test
    public void testSendMap() throws InterruptedException {
        // 准备消息
        Map<String, Object> msg = new HashMap<>();
        msg.put("name", "柳岩");
        msg.put("age", 21);
        // 发送消息
        rabbitTemplate.convertAndSend("object.queue", msg);
    }

    @Test
    void testPublisherConfirm() {
        CorrelationData cd = new CorrelationData();
        cd.getFuture().addCallback(new ListenableFutureCallback<CorrelationData.Confirm>() {
            @Override
            public void onFailure(Throwable ex) {
                log.error("send message fail", ex);
            }

            @Override
            public void onSuccess(@Nullable CorrelationData.Confirm result) {
                if (result.isAck()) {
                    log.debug("发送消息成功，收到ack!");
                } else {
                    log.error("发送消息失败，收到nack，reason:{}", result.getReason());
                }
            }
        });
        rabbitTemplate.convertAndSend("hmall.direct1", "blue", "hello", cd);
    }

    @Test
    void testPublisherDelayMessage() {
        // 1.创建消息
        String message = "hello, delayed message";
        // 2.发送消息，利用消息后置处理器添加消息头
        rabbitTemplate.convertAndSend("delay.direct", "delay", message, new MessagePostProcessor() {
            @Override
            public Message postProcessMessage(Message message) throws AmqpException {
                // 添加延迟消息属性
                message.getMessageProperties().setDelay(5000);
                return message;
            }
        });
    }
}
