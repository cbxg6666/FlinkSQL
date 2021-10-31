package com.cbxg.bean;

import lombok.*;

import java.io.Serializable;
import java.util.Date;

/**
 * @author:cbxg
 * @date:2021/7/29
 * @description:
 */
@Data
@NoArgsConstructor
public class Student2 implements Serializable {
    private String name = "zs";
    private int age = 1;
    private boolean is_cool = true;
    private double db = 0.000001;
//    public Student2(boolean is_cool){
//        this.is_cool = is_cool;
//    }
}
