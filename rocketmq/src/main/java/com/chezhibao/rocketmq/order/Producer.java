/*
 * Copyright (C), 2014-2017, 江苏乐博国际投资发展有限公司
 * FileName: Producer.java
 * Author:   zhangdanji
 * Date:     2017年12月31日
 * Description:
 */
package com.chezhibao.rocketmq.order;

import com.alibaba.rocketmq.client.producer.DefaultMQProducer;
import com.alibaba.rocketmq.client.producer.MessageQueueSelector;
import com.alibaba.rocketmq.client.producer.SendResult;
import com.alibaba.rocketmq.common.message.Message;
import com.alibaba.rocketmq.common.message.MessageQueue;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;

/**
 * @author zhangdanji
 */
public class Producer {
    public static void main(String[] args) {
        try {
            String groupName = "order_producer";
            DefaultMQProducer producer = new DefaultMQProducer(groupName);
            producer.setNamesrvAddr("master1:9876;master2:9876;follower1:9876;follower2:9876");
            producer.start();
            String date = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date());
            for(int i = 1; i <= 5; i++){
                String body = date + "order_1" + i;
                Message message = new Message("TopicTest", "order_1", "KEY" + i, body.getBytes());
                SendResult sendResult = producer.send(message, new MessageQueueSelector() {
                    @Override
                    public MessageQueue select(List<MessageQueue> mqs, Message msg, Object arg) {
                        int id = (int) arg;
                        return mqs.get(id);
                    }
                }, 0);
                System.out.println(sendResult + ",body:" + body);
            }
            for(int i = 1; i <= 5; i++){
                String body = date + "order_2" + i;
                Message message = new Message("TopicTest", "order_2", "KEY" + i, body.getBytes());
                SendResult sendResult = producer.send(message, new MessageQueueSelector() {
                    @Override
                    public MessageQueue select(List<MessageQueue> mqs, Message msg, Object arg) {
                        int id = (int) arg;
                        return mqs.get(id);
                    }
                }, 1);
                System.out.println(sendResult + ",body:" + body);
            }
            for(int i = 1; i <= 5; i++){
                String body = date + "order_3" + i;
                Message message = new Message("TopicTest", "order_3", "KEY" + i, body.getBytes());
                SendResult sendResult = producer.send(message, new MessageQueueSelector() {
                    @Override
                    public MessageQueue select(List<MessageQueue> mqs, Message msg, Object arg) {
                        int id = (int) arg;
                        return mqs.get(id);
                    }
                }, 2);
                System.out.println(sendResult + ",body:" + body);
            }
            producer.shutdown();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}