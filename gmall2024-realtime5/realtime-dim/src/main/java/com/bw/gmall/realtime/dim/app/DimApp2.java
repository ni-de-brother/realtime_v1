package com.bw.gmall.realtime.dim.app;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.bw.gmall.realtime.common.bean.TableProcessDim;
import com.bw.gmall.realtime.common.constant.Constant;
import com.bw.gmall.realtime.common.util.HBaseUtil2;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import netscape.javascript.JSObject;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;

import java.io.IOException;
import java.util.Properties;
/**
 * @Author xqy
 * @CreateTime 2024-12-13
 */

public class DimApp2 {
    public static void main(String[] args) throws Exception {
        //todo 1.基本环境准备
        //1.1 指定流处理环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //1.2 设置并行度
        env.setParallelism(4);
        //todo 2.检查相关的配置
        //2.1 开启检查点
        env.enableCheckpointing(5000L, CheckpointingMode.EXACTLY_ONCE);
        //2.2 设置检查点超市时间
        env.getCheckpointConfig().setCheckpointTimeout(60000L);
        env.getCheckpointConfig().setExternalizedCheckpointCleanup(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        //2.3 设置job取消后检查点是否保留
        //2.4 设置两个检查点之间最小时间间隔
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(2000L);
        //2.5 设置重启策略
        env.setRestartStrategy(RestartStrategies.failureRateRestart(3,Time.days(30),Time.seconds(3)));
        //2.6 设置状态后端以及检查点存储路径
        //       env.getCheckpointConfig().setCheckpointStorage("hdfs://hadoop102:8020/ck");
        env.setStateBackend(new HashMapStateBackend());
        //2.7 设置操作hadoop的用户
        //        System.setProperty("HADOOP_USER_NAME","root");
        //todo 3.从kafka的topic_db主题中获取业务数据
        //3.1 声明消费的主题以及消费者组
        String groupId="dim_app_group";
        // 3.2 创建消费者对象
        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers(Constant.KAFKA_BROKERS)
                .setTopics(Constant.TOPIC_DB)
                .setGroupId(groupId)
                //生产环境中,为了保证消费的精准一次性,手动维护偏移量
                //setStartingOffsets(OffsetsInitializer.committedOffsets(OffsetResetStrategy.EARLIEST))
                .setStartingOffsets(OffsetsInitializer.earliest())
                //.setValueOnlyDeserializer(new SimpleStringSchema())
                .setValueOnlyDeserializer(
                        new DeserializationSchema<String>() {
                            @Override
                            public String deserialize(byte[] message) throws IOException {
                                if(message != null){
                                    return new String(message);
                                }
                                return null;
                            }

                            @Override
                            public boolean isEndOfStream(String nextElement) {
                                return false;
                            }

                            @Override
                            public TypeInformation<String> getProducedType() {
                                return TypeInformation.of(String.class);
                            }
                        }
                )
                .build();
        //3.3 消费数据 封装为流
        DataStreamSource<String> kafka_source = env.fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka Source");
//env.execute();

        //todo 4 .对业务流中数据类型进行转换 jsonStr->jsonObj
        SingleOutputStreamOperator<JSONObject> jsonObjDS  = kafka_source.process(
                new ProcessFunction<String, JSONObject>() {
                    @Override
                    public void processElement(String s, ProcessFunction<String, JSONObject>.Context context, Collector<JSONObject> collector) throws Exception {
                        JSONObject jsonObject = JSON.parseObject(s);
                        String db = jsonObject.getString("database");
                        String type = jsonObject.getString("type");
                        String data = jsonObject.getString("data");
                        if ("gmall".equals(db)
                                && ("insert".equals(type)
                                || "update".equals(type)
                                || "delete".equals(type)
                                || "bootstrap-insert".equals(type))
                                && data != null
                                && data.length() > 2) {
                            collector.collect(jsonObject);
                        }
                    }
                }
        );
//        jsonObjDS.print();
//        env.execute();
        //todo 5.使用FlinkCdC 读取配置表中的配置信息
        //5.1 创建mysqlsource对象
        Properties props = new Properties();
        props.setProperty("useSSL", "false");
        props.setProperty("allowPublicKeyRetrieval", "true");
        MySqlSource<String> mySqlSource = MySqlSource.<String>builder()
                .hostname(Constant.MYSQL_HOST)
                .port(Constant.MYSQL_PORT)
                .databaseList("gmall_config")
                .tableList("gmall_config.table_process_dim")
                .username(Constant.MYSQL_USER_NAME)
                .password(Constant.MYSQL_PASSWORD)
                .deserializer(new JsonDebeziumDeserializationSchema())
                .startupOptions(StartupOptions.earliest())
                .jdbcProperties(props)
                .build();
        //5.2 读取数据 封装为流
        DataStreamSource<String> mySQL_source = env.fromSource(mySqlSource, WatermarkStrategy.noWatermarks(), "MySQL Source")
                .setParallelism(1);;
//         mySQL_source.print();
        //  {"before":null,"after":{"source_table":"1","sink_table":"1","sink_family":"1","sink_columns":"1","sink_row_key":"1"},"source":{"version":"1.9.7.Final","connector":"mysql","name":"mysql_binlog_source","ts_ms":1734073020000,"snapshot":"false","db":"gmall_config","sequence":null,"table":"table_process_dim","server_id":1,"gtid":null,"file":"mysql-bin.000151","pos":7264338,"row":0,"thread":24,"query":null},"op":"c","ts_ms":1734073020365,"transaction":null}
        //todo 6.对配置流中的数据类型进行转换 jsonStr->实体类对象
        SingleOutputStreamOperator<TableProcessDim> tpDs  = mySQL_source.map(
                new MapFunction<String, TableProcessDim>() {
                    @Override
                    public TableProcessDim map(String s) throws Exception {
                        //为了处理方便,先转换成jsonObj
                        JSONObject jsonObject = JSON.parseObject(s);
                        //拿到操作类型
                        String op = jsonObject.getString("op");
                        TableProcessDim tableProcessDim = null;
                        if ("d".equals(op)) {
                            //对配置表进行了一次删除操作.从before中获取
                            tableProcessDim = jsonObject.getObject("before", TableProcessDim.class);
                        } else {
                            //对配置表进行了读取,添加,修改,从after中获取
                            tableProcessDim = jsonObject.getObject("after", TableProcessDim.class);
                        }
                        tableProcessDim.setOp(op);
                        return tableProcessDim;
                    }
                }
        ).setParallelism(1);
        //tpDs.print();
        //todo 7. 根据配置表中的配置信息到Hbase中执行建表和删除表的操作
        SingleOutputStreamOperator<TableProcessDim> createHbase = tpDs.map(
                new RichMapFunction<TableProcessDim, TableProcessDim>() {

                    private Connection hbaseConn;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        hbaseConn = HBaseUtil2.getHBaseConnection();
                    }

                    @Override
                    public void close() throws Exception {
                        HBaseUtil2.closeHBaseConnection(hbaseConn);
                    }

                    @Override
                    public TableProcessDim map(TableProcessDim tp) throws Exception {
                        //获取对配置表进仓的操的类型
                        String op = tp.getOp();
                        //hbase中的列族
                        String[] sinkFamilies = tp.getSink_family().split(",");
                        //获取表名字
                        String sinkTable = tp.getSink_table();
                        if ("d".equals(op)) {
                            //删除一个数据
                            HBaseUtil2.dropHBaseTable(hbaseConn, Constant.HBASE_NAMESPACE, sinkTable);

                        } else if ("r".equals(op) || "c".equals(op)) {
                            //从配置表中读取or添加一条
                            HBaseUtil2.createHBaseTable(hbaseConn, Constant.HBASE_NAMESPACE, sinkTable, sinkFamilies);
                        } else {
                            //对配置表中的配置信息进行了修改,修改(先删除表,再添加)
                            HBaseUtil2.dropHBaseTable(hbaseConn, Constant.HBASE_NAMESPACE, sinkTable);
                            HBaseUtil2.createHBaseTable(hbaseConn, Constant.HBASE_NAMESPACE, sinkTable, sinkFamilies);
                        }

                        return tp;
                    }
                }
        );
//        createHbase.print();
        //todo 8.将配置流中的配置信息进行广播---broadcast

        //todo 9.将主流业务数据和广播流配置信息进行关联---connect
        env.execute();
    }
}
