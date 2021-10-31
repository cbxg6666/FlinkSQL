package com.cbxg.bean;

import com.alibaba.fastjson.JSON;
import com.cbxg.bean.WaterSensor;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

//package com.cbxg.sql.bean;
//
//import com.alibaba.fastjson.JSON;
//import com.alibaba.fastjson.JSONObject;
//import com.alibaba.fastjson.annotation.JSONField;
//import lombok.AllArgsConstructor;
//import lombok.Data;
//
//import java.util.ArrayList;
//import java.util.Date;
//import java.util.List;
//
///**
// * @author:cbxg
// * @date:2021/7/21
// * @description:
// */
////@Data
//public class Person {
//
//    @JSONField(name = "AGE")
//    private int age;
//
//    @JSONField(name = "FULL NAME")
//    private String fullName;
//
//    @JSONField(name = "DATE OF BIRTH")
//    private Date dateOfBirth;
//
//    public Person(int age, String fullName, Date dateOfBirth) {
//        super();
//        this.age = age;
//        this.fullName= fullName;
//        this.dateOfBirth = dateOfBirth;
//    }
//
//    // 标准 getters & setters
//
//    public static void main(String[] args) {
//        final Person person = new Person(15, "John Doe", new Date());
//        System.out.println(JSON.toJSONString(person));
//    }
//
//}
//
//
@Data
@NoArgsConstructor

public class Person{
    private String name;
    private int age;
    private String ws;
    public Person(String name, int age, WaterSensor ws){
        this.name = name;
        this.age = age;
        this.ws = JSON.toJSONString(ws);
    }
}