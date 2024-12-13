package com.bw.gmall.realtime.common.bean;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
/**
 * @Author xqy
 * @CreateTime 2024-12-12
 */

public class TableProcessDim {
    // 来源表名
    String source_table;
    // 目标表名
    String sink_table;

    // 输出字段
    String sink_columns;

    // 数据到 hbase 的列族
    String sink_family;

    // sink到 hbase 的时候的主键字段
    String sink_row_key;

    // 配置表操作类型
    String op;

}
