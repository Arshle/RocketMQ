/*
 * Copyright (C), 2014-2017, 江苏乐博国际投资发展有限公司
 * FileName: Consumer1.java
 * Author:   zhangdanji
 * Date:     2017年12月30日
 * Description:
 */
package com.chezhibao.rocketmq.model;

import com.alibaba.rocketmq.client.consumer.DefaultMQPushConsumer;
import com.alibaba.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import com.alibaba.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import com.alibaba.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import com.alibaba.rocketmq.client.consumer.rebalance.AllocateMessageQueueAveragely;
import com.alibaba.rocketmq.common.message.MessageExt;
import com.alibaba.rocketmq.common.protocol.heartbeat.MessageModel;

import java.util.List;

/**
 * @author zhangdanji
 */
public class Consumer1 {
    private final static String GROUP_NAME = "message_consumer";

    Consumer1(){
        try {
            DefaultMQPushConsumer consumer = new DefaultMQPushConsumer(GROUP_NAME);
            consumer.setNamesrvAddr("follower1:9876;follower2:9876;follower:9876");
            consumer.subscribe("Topic1","Tag1 || Tag2 || Tag3");
            consumer.registerMessageListener(new Listener());
            consumer.setAllocateMessageQueueStrategy(new AllocateMessageQueueAveragely());
            consumer.setMessageModel(MessageModel.BROADCASTING);
            consumer.start();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    class Listener implements MessageListenerConcurrently{

        @Override
        public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgs, ConsumeConcurrentlyContext context) {
            for(MessageExt msg : msgs){
                try {
                    String topic = msg.getTopic();
                    String msgBody = new String(msg.getBody(),"UTF-8");
                    String tags = msg.getTags();
                    System.out.println("收到消息,topic:" + topic + ",msgBody:" + msgBody + ",tags:" + tags);
                } catch (Exception e) {
                    e.printStackTrace();
                    return ConsumeConcurrentlyStatus.RECONSUME_LATER;
                }
            }
            return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
        }
    }

    public static void main(String[] args) {
        Consumer1 consumer = new Consumer1();
        System.out.println("consumer start.");
    }
}
