package com.cbxg.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @author:cbxg
 * @date:2021/9/12
 * @description:
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class LoginEvent   {
    private  String userId;
    private String  ip;
    private  String flag;
    private String eventTime;


}
