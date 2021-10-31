package com.cbxg.bean;

import java.util.HashMap;

/**
 * @author:cbxg
 * @date:2021/7/22
 * @description:
 */
public class Test {
    public static void main(String[] args) {

        Class<Test> testClass = Test.class;
        final Tmp<Person> personTmp = new Tmp<>();
        personTmp.test(new Person(),Person.class);



    }
}
class Tmp<T>{

    public void  test(T t,Class<T> o){
        final Class<Person> personClass = Person.class;
        System.out.println(o.equals(personClass));
    }
}