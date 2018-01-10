/*
 * Copyright (C), 2014-2017, 江苏乐博国际投资发展有限公司
 * FileName: MessageFilterImpl.java
 * Author:   zhangdanji
 * Date:     2018年01月01日
 * Description:
 */
package com.chezhibao.rocketmq.filter;

import com.alibaba.rocketmq.common.filter.MessageFilter;
import com.alibaba.rocketmq.common.message.MessageExt;

/**
 * @author zhangdanji
 */
public class MessageFilterImpl implements MessageFilter{

    @Override
    public boolean match(MessageExt messageExt) {
        String property = messageExt.getProperty("SequenceId");
        if(property != null){
            int id = Integer.parseInt(property);
            return id % 3 == 0 && id > 10;
        }
        return false;
    }
}
