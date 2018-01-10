/*
 * Copyright (C), 2014-2017, 江苏乐博国际投资发展有限公司
 * FileName: Consumer.java
 * Author:   zhangdanji
 * Date:     2017年12月31日
 * Description:
 */
package com.chezhibao.rocketmq.transaction;

import com.alibaba.rocketmq.client.consumer.DefaultMQPushConsumer;
import com.alibaba.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import com.alibaba.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import com.alibaba.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import com.alibaba.rocketmq.common.message.MessageExt;
import java.util.List;

/**
 * @author zhangdanji
 */
public class Consumer {
    public Consumer(){
        try {
            String groupName = "transaction_producer";
            DefaultMQPushConsumer consumer = new DefaultMQPushConsumer(groupName);
            consumer.setNamesrvAddr("master1:9876;master2:9876;follower1:9876;follower2:9876");
            consumer.subscribe("TopicTransaction","*");
            consumer.registerMessageListener(new Listener());
            consumer.start();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    class Listener implements MessageListenerConcurrently{

        @Override
        public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgs, ConsumeConcurrentlyContext context) {
            try {
                for(MessageExt msg : msgs){
                    try {
                        String topic = msg.getTopic();
                        String msgBody = new String(msg.getBody(),"UTF-8");
                        String tags = msg.getTags();
                        System.out.println("收到消息,topic:" + topic + ",msgBody:" + msgBody + ",tags:" + tags);
                    } catch ( Exception e) {
                        e.printStackTrace();
                    }
                }
                return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
            } catch (Exception e) {
                e.printStackTrace();
            }
            return ConsumeConcurrentlyStatus.RECONSUME_LATER;
        }
    }

    public static void main(String[] args) {
        Consumer consumer = new Consumer();
        System.out.println("consumer start.");
    }
}
