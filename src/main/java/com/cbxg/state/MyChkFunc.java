//package com.cbxg.state;
//
//import org.apache.flink.api.common.state.ListState;
//import org.apache.flink.api.common.state.ListStateDescriptor;
//import org.apache.flink.api.common.typeinfo.TypeHint;
//import org.apache.flink.api.common.typeinfo.TypeInformation;
//import org.apache.flink.api.java.tuple.Tuple2;
//import org.apache.flink.runtime.state.FunctionInitializationContext;
//import org.apache.flink.runtime.state.FunctionSnapshotContext;
//import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
//
///**
// * @author:cbxg
// * @date:2021/8/3
// * @description:
// */
//public class MyChkFunc implements CheckpointedFunction {
//
//    @Override
//    public void snapshotState(FunctionSnapshotContext context) throws Exception {
//
//    }
//
//    @Override
//    public void initializeState(FunctionInitializationContext context) throws Exception {
//        ListStateDescriptor<Tuple2<String, Integer>> descriptor =
//                new ListStateDescriptor<>(
//                        "buffered-elements",
//                        TypeInformation.of(new TypeHint<Tuple2<String, Integer>>() {}));
//
//        ListState<Tuple2<String, Integer>> checkpointedState = context.getOperatorStateStore().getListState(descriptor);
//
//        if (context.isRestored()) {
//            for (Tuple2<String, Integer> element : checkpointedState.get()) {
//                bufferedElements.add(element);
//            }
//        }
//    }
//}
