//package com.cbxg.bean;
//
//import org.apache.flink.orc.vector.Vectorizer;
//import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;
//import org.apache.hadoop.hive.ql.exec.vector.ColumnVector;
//import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
//import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
//
//import java.io.IOException;
//
///**
// * @author:cbxg
// * @date:2021/7/30
// * @description:
// */
//public class Studnet2Vectorizer extends Vectorizer<Student2> {
//
//    public Studnet2Vectorizer(String schema) {
//        super(schema);
//    }
//
//    @Override
//    public void vectorize(Student2 student2, VectorizedRowBatch batch) throws IOException {
//        int id = batch.size++;
//
//        byte[] bytes = student2.getName().getBytes();
//        ((BytesColumnVector) batch.cols[0]).setVal(id, bytes, 0, bytes.length);
//
//        int age = student2.getAge();
//        ((LongColumnVector) batch.cols[1]).setElement(0,1,this);
//        final boolean cool = student2.is_cool();
//        ((BytesColumnVector) batch.cols[2]).setVal(id, bytes, 0, bytes.length);
//
//
//    }
//}
