/*
 * Copyright (C), 2014-2017, 江苏乐博国际投资发展有限公司
 * FileName: Consumer.java
 * Author:   zhangdanji
 * Date:     2017年12月31日
 * Description:
 */
package com.chezhibao.rocketmq.order;

import com.alibaba.rocketmq.client.consumer.DefaultMQPushConsumer;
import com.alibaba.rocketmq.client.consumer.listener.ConsumeOrderlyContext;
import com.alibaba.rocketmq.client.consumer.listener.ConsumeOrderlyStatus;
import com.alibaba.rocketmq.client.consumer.listener.MessageListenerOrderly;
import com.alibaba.rocketmq.common.consumer.ConsumeFromWhere;
import com.alibaba.rocketmq.common.message.MessageExt;

import java.io.UnsupportedEncodingException;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;

/**
 * @author zhangdanji
 */
public class Consumer1 {
    public Consumer1(){
        try {
            String groupName = "order_consumer";
            DefaultMQPushConsumer consumer = new DefaultMQPushConsumer(groupName);
            consumer.setNamesrvAddr("master1:9876;master2:9876;follower1:9876;follower2:9876");
            consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);
            consumer.subscribe("TopicTest","*");
            consumer.registerMessageListener(new Listener());
            consumer.start();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    class Listener implements MessageListenerOrderly{

        private Random random = new Random();

        @Override
        public ConsumeOrderlyStatus consumeMessage(List<MessageExt> msgs, ConsumeOrderlyContext context) {
            context.setAutoCommit(true);
            for(MessageExt msg : msgs){
                try {
                    System.out.println(msg + ",content:" + new String(msg.getBody(),"UTF-8"));
                } catch (UnsupportedEncodingException e) {
                    e.printStackTrace();
                }
                try {
                    TimeUnit.SECONDS.sleep(random.nextInt(10));
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
            return ConsumeOrderlyStatus.SUCCESS;
        }
    }

    public static void main(String[] args) {
        Consumer1 consumer1 = new Consumer1();
        System.out.println("consumer1 start.");
    }
}
