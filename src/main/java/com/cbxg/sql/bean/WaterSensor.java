package com.cbxg.sql.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @author:cbxg
 * @date:2021/4/5
 * @description:
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class WaterSensor {

    private  String id;
    private  Long ts;
    private  int vc;
}
