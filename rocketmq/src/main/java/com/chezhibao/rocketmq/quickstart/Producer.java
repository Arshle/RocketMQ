/*
 * Copyright (C), 2014-2017, 江苏乐博国际投资发展有限公司
 * FileName: Producer.java
 * Author:   zhangdanji
 * Date:     2017年12月29日
 * Description:
 */
package com.chezhibao.rocketmq.quickstart;

import com.alibaba.rocketmq.client.exception.MQClientException;
import com.alibaba.rocketmq.client.producer.DefaultMQProducer;
import com.alibaba.rocketmq.client.producer.SendResult;
import com.alibaba.rocketmq.common.message.Message;

/**
 * @author zhangdanji
 */
public class Producer {
    public static void main(String[] args) throws MQClientException, InterruptedException {
        DefaultMQProducer producer = new DefaultMQProducer("quickstart_producer");
        producer.setNamesrvAddr("master1:9876;master2:9876;follower1:9876;follower2:9876");
        producer.start();

        for(int i = 0; i < 10; i ++){
            try {
                Message message = new Message("TopicQuickStart","TagA",("Hello RocketMQ" + i).getBytes());
                SendResult sendResult = producer.send(message);
                System.out.println(sendResult);
            } catch (Exception e) {
                e.printStackTrace();
                Thread.sleep(1000);
            }
        }

        producer.shutdown();
    }
}
