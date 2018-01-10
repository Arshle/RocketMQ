/*
 * Copyright (C), 2014-2017, 江苏乐博国际投资发展有限公司
 * FileName: Producer.java
 * Author:   zhangdanji
 * Date:     2017年12月31日
 * Description:
 */
package com.chezhibao.rocketmq.transaction;

import com.alibaba.rocketmq.client.producer.LocalTransactionState;
import com.alibaba.rocketmq.client.producer.TransactionCheckListener;
import com.alibaba.rocketmq.client.producer.TransactionMQProducer;
import com.alibaba.rocketmq.client.producer.TransactionSendResult;
import com.alibaba.rocketmq.common.message.Message;
import com.alibaba.rocketmq.common.message.MessageExt;

import java.util.concurrent.TimeUnit;

/**
 * @author zhangdanji
 */
public class Producer {
    public static void main(String[] args) {
        try {
            String groupName = "transaction_producer";
            final TransactionMQProducer producer = new TransactionMQProducer(groupName);
            producer.setNamesrvAddr("master1:9876;master2:9876;follower1:9876;follower2:9876");
            producer.setCheckThreadPoolMinSize(5);
            producer.setCheckThreadPoolMaxSize(20);
            producer.setCheckRequestHoldMax(2000);
            producer.start();
            //服务器自动回调，查看本地是成功是失败，可写自己的业务逻辑
            producer.setTransactionCheckListener(new TransactionCheckListener() {
                @Override
                public LocalTransactionState checkLocalTransactionState(MessageExt msg) {
                    try {
                        System.out.println("state---" + new String(msg.getBody(),"UTF-8"));
                        return LocalTransactionState.COMMIT_MESSAGE;
                    } catch (Exception e) {
                        e.printStackTrace();
                        return LocalTransactionState.ROLLBACK_MESSAGE;
                    }
                }
            });
            TransactionExecuterImpl tranExecuter = new TransactionExecuterImpl();
            for(int i = 1; i <= 2; i++ ){
                try {
                    Message message = new Message("TopicTransaction","Transaction" + i,"key",("HelloRocket" + i).getBytes());
                    TransactionSendResult sendResult = producer.sendMessageInTransaction(message, tranExecuter, "tq");
                    System.out.println(sendResult);
                } catch (Exception e) {
                    e.printStackTrace();
                }
                TimeUnit.SECONDS.sleep(1);
            }
            Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
                @Override
                public void run() {
                    producer.shutdown();
                }
            }));
            System.exit(0);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
