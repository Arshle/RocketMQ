/*
 * Copyright (C), 2014-2017, 江苏乐博国际投资发展有限公司
 * FileName: PullScheduleService.java
 * Author:   zhangdanji
 * Date:     2018年01月01日
 * Description:
 */
package com.chezhibao.rocketmq.pull;

import com.alibaba.rocketmq.client.consumer.*;
import com.alibaba.rocketmq.client.exception.MQClientException;
import com.alibaba.rocketmq.common.message.MessageExt;
import com.alibaba.rocketmq.common.message.MessageQueue;
import com.alibaba.rocketmq.common.protocol.heartbeat.MessageModel;

import java.util.List;

/**
 * @author zhangdanji
 */
public class PullScheduleService {
    public static void main(String[] args) throws MQClientException {
        String groupName = "schedule_consumer";
        final MQPullConsumerScheduleService scheduleService = new MQPullConsumerScheduleService(groupName);
        scheduleService.getDefaultMQPullConsumer().setNamesrvAddr("master1:9876;master2:9876;follower1:9876;follower2:9876");
        scheduleService.setMessageModel(MessageModel.CLUSTERING);
        scheduleService.registerPullTaskCallback("TopicPull", new PullTaskCallback() {
            @Override
            public void doPullTask(MessageQueue mq, PullTaskContext context) {
                MQPullConsumer consumer = context.getPullConsumer();
                try {
                    long offset = consumer.fetchConsumeOffset(mq, false);
                    if(offset < 0){
                        offset = 0;
                    }
                    PullResult pullResult = consumer.pull(mq, "*", offset, 32);
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
                        case OFFSET_ILLEGAL:
                            break;
                        default:
                            break;
                    }

                    consumer.updateConsumeOffset(mq,pullResult.getNextBeginOffset());

                    context.setPullNextDelayTimeMillis(1000);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        });
        scheduleService.start();
    }
}
