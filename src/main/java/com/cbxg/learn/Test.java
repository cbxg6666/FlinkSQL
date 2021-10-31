package com.cbxg.learn;

import com.cbxg.bean.LoginEvent;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.functions.PatternProcessFunction;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.time.Duration;
import java.util.List;
import java.util.Map;

/**
 * @author:cbxg
 * @date:2021/9/12
 * @description:
 */
public class Test {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.setParallelism(1);
        final KeyedStream<LoginEvent, String> input = env.fromElements(
                new LoginEvent("1", "0.0.0.0", "fail", "1"),
                new LoginEvent("1", "0.0.0.0", "success", "2"),
                new LoginEvent("1", "0.0.0.0", "fail", "3"),
                new LoginEvent("1", "0.0.0.0", "fail", "4")
        ).assignTimestampsAndWatermarks(
                WatermarkStrategy.<LoginEvent>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                        .withTimestampAssigner((element, recordTimestamp) -> Long.parseLong(element.getEventTime()) * 1000)
        ).keyBy(LoginEvent::getUserId);

        input.print();
        final Pattern<LoginEvent, LoginEvent> pattern = Pattern.<LoginEvent>begin("firstFail").where(
                new SimpleCondition<LoginEvent>() {
                    @Override
                    public boolean filter(LoginEvent event) {
                        return event.getFlag() == "fail";
                    }
                }
        ).next("secondFail").where(
                new SimpleCondition<LoginEvent>() {
                    @Override
                    public boolean filter(LoginEvent event) {
                        return event.getFlag() == "fail";
                    }
                }
        ).within(Time.seconds(5));

        PatternStream<LoginEvent> patternStream = CEP.pattern(input, pattern);

        final SingleOutputStreamOperator<LoginEvent> firstFail = patternStream.select(new PatternSelectFunction<LoginEvent, LoginEvent>() {
            @Override
            public LoginEvent select(Map<String, List<LoginEvent>> map) throws Exception {
                final List<LoginEvent> firstFail = map.get("firstFail");
                System.out.println("=="+firstFail);
                final LoginEvent loginEvent = firstFail.get(0);
                return loginEvent;
            }
        });
        firstFail.print();


        env.execute();
    }
}