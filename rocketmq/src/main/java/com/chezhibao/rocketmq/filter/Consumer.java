/*
 * Copyright (C), 2014-2017, 江苏乐博国际投资发展有限公司
 * FileName: Consumer.java
 * Author:   zhangdanji
 * Date:     2018年01月01日
 * Description:
 */
package com.chezhibao.rocketmq.filter;

import com.alibaba.rocketmq.client.consumer.DefaultMQPushConsumer;
import com.alibaba.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import com.alibaba.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import com.alibaba.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import com.alibaba.rocketmq.client.exception.MQClientException;
import com.alibaba.rocketmq.common.MixAll;
import com.alibaba.rocketmq.common.message.MessageExt;

import java.io.UnsupportedEncodingException;
import java.util.List;

/**
 * @author zhangdanji
 */
public class Consumer {
    public static void main(String[] args) {
        try {
            String groupName = "filter_consumer";
            DefaultMQPushConsumer consumer = new DefaultMQPushConsumer(groupName);
            consumer.setNamesrvAddr("master1:9876;master2:9876;follower1:9876;follower2:9876");
            String filterCoder = MixAll.file2String("/Users/zhangdanji/IdeaProjects/RocketMQ/rocketmq/src/main/java/com/chezhibao/rocketmq/filter/MessageFilterImpl.java");
            System.out.println(filterCoder);
            consumer.subscribe("TopicFilter7","com.chezhibao.rocketmq.filter.MessageFileterImpl",filterCoder);
            //consumer.subscribe("TopicFilter7",MessageFilterImpl.class.getCanonicalName());
            consumer.registerMessageListener(new MessageListenerConcurrently() {
                @Override
                public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgs, ConsumeConcurrentlyContext context) {
                    try {
                        System.out.println(new String(msgs.get(0).getBody(),"UTF-8"));
                        return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
                    } catch (UnsupportedEncodingException e) {
                        e.printStackTrace();
                        return ConsumeConcurrentlyStatus.RECONSUME_LATER;
                    }
                }
            });
            consumer.start();
            System.out.println("consumer start.");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
