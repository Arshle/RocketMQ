/*
 * Copyright (C), 2014-2017, 江苏乐博国际投资发展有限公司
 * FileName: Producer.java
 * Author:   zhangdanji
 * Date:     2017年12月30日
 * Description:
 */
package com.chezhibao.rocketmq.model;

import com.alibaba.rocketmq.client.exception.MQClientException;
import com.alibaba.rocketmq.client.producer.DefaultMQProducer;
import com.alibaba.rocketmq.client.producer.SendResult;
import com.alibaba.rocketmq.common.message.Message;

/**
 * @author zhangdanji
 */
public class Producer {
    private final static String GROUP_NAME = "message_producer";

    public static void main(String[] args) throws MQClientException {
        DefaultMQProducer producer = new DefaultMQProducer(GROUP_NAME);
        producer.setNamesrvAddr("follower1:9876;follower2:9876;follower3:9876");
        producer.start();

        for(int i = 1; i <= 1; i ++){
            try {
                Message message = new Message("Topic1","Tag1",("信息内容" + i).getBytes());
                SendResult sendResult = producer.send(message);
                System.out.println(sendResult);
            } catch (Exception e) {
                e.printStackTrace();
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e1) {
                    e1.printStackTrace();
                }
            }

        }
        producer.shutdown();
    }
}
