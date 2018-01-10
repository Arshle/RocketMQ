/*
 * Copyright (C), 2014-2017, 江苏乐博国际投资发展有限公司
 * FileName: Producer.java
 * Author:   zhangdanji
 * Date:     2018年01月01日
 * Description:
 */
package com.chezhibao.rocketmq.pull;

import com.alibaba.rocketmq.client.producer.DefaultMQProducer;
import com.alibaba.rocketmq.client.producer.SendResult;
import com.alibaba.rocketmq.common.message.Message;

/**
 * @author zhangdanji
 */
public class Producer {
    public static void main(String[] args) {
        try {
            String groupName = "pull_producer";
            DefaultMQProducer producer = new DefaultMQProducer(groupName);
            producer.setNamesrvAddr("master1:9876;master2:9876;follower1:9876;follower2:9876");
            producer.start();
            for(int i = 0; i < 5; i ++){
                Message message = new Message("TopicPull","Tag",("HelloRocket" + i).getBytes());
                SendResult sendResult = producer.send(message);
                System.out.println(sendResult);
                Thread.sleep(1000);
            }
            producer.shutdown();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
