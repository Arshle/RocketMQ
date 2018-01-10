/*
 * Copyright (C), 2014-2017, 江苏乐博国际投资发展有限公司
 * FileName: TransactionExecuterImpl.java
 * Author:   zhangdanji
 * Date:     2017年12月31日
 * Description:
 */
package com.chezhibao.rocketmq.transaction;

import com.alibaba.rocketmq.client.producer.LocalTransactionExecuter;
import com.alibaba.rocketmq.client.producer.LocalTransactionState;
import com.alibaba.rocketmq.common.message.Message;

/**
 * @author zhangdanji
 */
public class TransactionExecuterImpl implements LocalTransactionExecuter {
    @Override
    public LocalTransactionState executeLocalTransactionBranch(Message msg, Object arg) {
        try {
            System.out.println("msg="+ new String(msg.getBody(),"UTF-8"));
            System.out.println("arg=" + arg);
            String tag = msg.getTags();
            if("Transaction1".equals(tag)){
                System.out.println("处理业务逻辑");
                return LocalTransactionState.ROLLBACK_MESSAGE;
            }
            return LocalTransactionState.COMMIT_MESSAGE;
        } catch (Exception e) {
            e.printStackTrace();
            return LocalTransactionState.ROLLBACK_MESSAGE;
        }
    }
}
