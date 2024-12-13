package com.bw.gmall.realtime.dim.app;

/**
 * @Author xqy
 * @CreateTime 2024-12-12
 */
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.bw.gmall.realtime.common.base.BaseApp;
import com.bw.gmall.realtime.common.bean.TableProcessDim;
import com.bw.gmall.realtime.common.constant.Constant;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Properties;

@Slf4j
public class DimApp extends BaseApp{
    public static void main(String[] args) {
        new DimApp().start(10001,
                4,
                "dim_app",
                Constant.TOPIC_DB
        );
//        SingleOutputStreamOperator<TableProcessDim> configStream = readTableProcess(env);

    }

    @Override
    public void handle(StreamExecutionEnvironment env,
                       DataStreamSource<String> stream) {
        // 1. 对消费的数据, 做数据清洗
        SingleOutputStreamOperator<JSONObject> etlStream = etl(stream);
    }
    private SingleOutputStreamOperator<JSONObject> etl(DataStreamSource<String> stream) {
        return stream
                .filter(new FilterFunction<String>() {
                    @Override
                    public boolean filter(String value) {
                        try {
                            JSONObject jsonObj = JSON.parseObject(value);
                            String db = jsonObj.getString("database");
                            String type = jsonObj.getString("type");
                            String data = jsonObj.getString("data");

                            return "gmall".equals(db)
                                    && ("insert".equals(type)
                                    || "update".equals(type)
                                    || "delete".equals(type)
                                    || "bootstrap-insert".equals(type))
                                    && data != null
                                    && data.length() > 2;

                        } catch (Exception e) {
                            log.warn("不是正确的 json 格式的数据: " + value);
                            return false;
                        }

                    }
                })
                .map(JSON::parseObject);
    }
    private  SingleOutputStreamOperator<TableProcessDim> readTableProcess(StreamExecutionEnvironment env) {
        // useSSL=false
        Properties props = new Properties();
        props.setProperty("useSSL", "false");
        props.setProperty("allowPublicKeyRetrieval", "true");
        MySqlSource<String> mySqlSource = MySqlSource.<String>builder()
                .hostname(Constant.MYSQL_HOST)
                .port(Constant.MYSQL_PORT)
                .databaseList("gmall_config") // set captured database, If you need to synchronize the whole database, Please set tableList to ".*".
                .tableList("gmall_config.table_process_dim") // set captured table
                .username(Constant.MYSQL_USER_NAME)
                .password(Constant.MYSQL_PASSWORD)
                .jdbcProperties(props)
                .deserializer(new JsonDebeziumDeserializationSchema()) // converts SourceRecord to JSON String
                .startupOptions(StartupOptions.earliest()) // 默认值: initial  第一次启动读取所有数据(快照), 然后通过 binlog 实时监控变化数据
                .build();

        return env
                .fromSource(mySqlSource, WatermarkStrategy.noWatermarks(), "cdc-source")
                .setParallelism(1) // 并行度设置为 1
                .map(new MapFunction<String, TableProcessDim>() {
                    @Override
                    public TableProcessDim map(String value) throws Exception {
                        JSONObject obj = JSON.parseObject(value);
                        String op = obj.getString("op");
                        TableProcessDim tableProcessDim;
                        if ("d".equals(op)) {
                            tableProcessDim = obj.getObject("before", TableProcessDim.class);
                        } else {
                            tableProcessDim = obj.getObject("after", TableProcessDim.class);
                        }
                        tableProcessDim.setOp(op);

                        return tableProcessDim;
                    }
                })
                .setParallelism(1);
    }
}
