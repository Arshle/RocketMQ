/*
 * Copyright (C), 2014-2017, 江苏乐博国际投资发展有限公司
 * FileName: Consumer.java
 * Author:   zhangdanji
 * Date:     2017年12月29日
 * Description:
 */
package com.chezhibao.rocketmq.quickstart;

import com.alibaba.rocketmq.client.consumer.DefaultMQPushConsumer;
import com.alibaba.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import com.alibaba.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import com.alibaba.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import com.alibaba.rocketmq.client.exception.MQClientException;
import com.alibaba.rocketmq.common.consumer.ConsumeFromWhere;
import com.alibaba.rocketmq.common.message.MessageExt;
import com.alibaba.rocketmq.common.protocol.heartbeat.MessageModel;

import java.io.UnsupportedEncodingException;
import java.util.List;

/**
 * @author zhangdanji
 */
public class Consumer {
    public static void main(String[] args) throws MQClientException {
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("quickstart_consumer");
        consumer.setNamesrvAddr("master1:9876;master2:9876;follower1:9876;follower2:9876");
        consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);
        consumer.subscribe("TopicQuickStart","*");
        consumer.setMessageModel(MessageModel.CLUSTERING);
        consumer.registerMessageListener(new MessageListenerConcurrently() {
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
                        if(msg.getReconsumeTimes() == 3){
                            return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
                        }
                        return ConsumeConcurrentlyStatus.RECONSUME_LATER;
                    }
                }
                return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
            }
        });
        consumer.start();
        System.out.println("consumer start.");
    }
}
