package com.cbxg.learn;

import com.cbxg.bean.LoginEvent;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * @author:cbxg
 * @date:2021/9/12
 * @description:
 */
public class MatchFunction extends KeyedProcessFunction<String, LoginEvent,String> {

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
//        getRuntimeContext().
    }

    @Override
    public void processElement(LoginEvent value, Context ctx, Collector<String> out) throws Exception {

        // 如果5s内连续出现3次登录失败情况 ，触发报警
        // user 失败了 注册5s定时器 报警 ， 如果5s内出现成功 删除定时器
//        状态记录失败次数和失败时间戳，
        ctx.timerService().deleteProcessingTimeTimer(1000L);
        ctx.timerService().registerEventTimeTimer( Long.parseLong(value.getEventTime()) * 1000 + 5*1000L);
    }

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
        super.onTimer(timestamp, ctx, out);
    }
}
