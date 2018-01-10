/*
 * Copyright (C), 2014-2017, 江苏乐博国际投资发展有限公司
 * FileName: PullConsumer.java
 * Author:   zhangdanji
 * Date:     2018年01月01日
 * Description:
 */
package com.chezhibao.rocketmq.pull;

import com.alibaba.rocketmq.client.consumer.DefaultMQPullConsumer;
import com.alibaba.rocketmq.client.consumer.PullResult;
import com.alibaba.rocketmq.common.message.MessageExt;
import com.alibaba.rocketmq.common.message.MessageQueue;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * @author zhangdanji
 */
public class PullConsumer {
    private static final Map<MessageQueue, Long> OFFSET_TABLE = new HashMap<>();

    public static void main(String[] args) {
        try {
            String groupName = "pull_consumer";
            DefaultMQPullConsumer consumer = new DefaultMQPullConsumer(groupName);
            consumer.setNamesrvAddr("master1:9876;master2:9876;follower1:9876;follower2:9876");
            consumer.start();
            Set<MessageQueue> mqs = consumer.fetchSubscribeMessageQueues("TopicPull");
            for(MessageQueue mq : mqs){
                System.out.println("Consume from the queue:" + mq);
                SINGLE_MQ:while(true){
                    try {
                        PullResult pullResult = consumer.pullBlockIfNotFound(mq, null, getMessageQueueOffset(mq), 32);
                        System.out.println(pullResult);
                        System.out.println(pullResult.getPullStatus());
                        putMessageQueueOffset(mq,pullResult.getNextBeginOffset());
                        switch (pullResult.getPullStatus()){
                            case FOUND:
                                List<MessageExt> list = pullResult.getMsgFoundList();
                                for(MessageExt msg : list){
                                    System.out.println(new String(msg.getBody(),"UTF-8"));
                                }
                                break;
                            case NO_MATCHED_MSG:
                                break;
                            case NO_NEW_MSG:
                                System.out.println("没有新的消息...");
                                break SINGLE_MQ;
                            case OFFSET_ILLEGAL:
                                break;
                            default:
                                break;
                        }
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static void putMessageQueueOffset(MessageQueue mq,long offset){
        OFFSET_TABLE.put(mq,offset);
    }

    private static long getMessageQueueOffset(MessageQueue mq){
        Long offset = OFFSET_TABLE.get(mq);
        if(offset != null){
            return offset;
        }
        return 0;
    }
}
