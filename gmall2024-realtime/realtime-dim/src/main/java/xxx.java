import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @Author xqy
 * @CreateTime 2024-12-12
 */

public class xxx {
    public static void main(String[] args) throws Exception {
//        //基本环境准备
//        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        //设置并行度
//        env.setParallelism(1);
//        env.enableCheckpointing(3000);
//        //使用FlinkCDC读取mysql表中的数据
//        MySqlSource<String> mySqlSource = MySqlSource.<String>builder()
//                .hostname("hadoop102")
//                .port(3306)
//                .databaseList("gmall") // set captured database
//                .tableList("gmall.order_info") // set captured table
//                .username("root")
//                .password("000000")
////                .startupOptions(StartupOptions.initial())
////                .startupOptions(StartupOptions.earliest())
//                .deserializer(new JsonDebeziumDeserializationSchema()) // converts SourceRecord to JSON String
//                .build();
//
//
//
//        env.fromSource(mySqlSource, WatermarkStrategy.noWatermarks(), "MySQL Source")
//                // set 4 parallel source tasks
//                .setParallelism(4)
//                .print();// use parallelism 1 for sink to keep message ordering
//        env.execute();
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.enableCheckpointing(3000);

        MySqlSource<String> mySqlSource = MySqlSource.<String>builder()
                .hostname("hadoop102")
                .port(3306)
                .databaseList("gmall_config") // 设置捕获的数据库， 如果需要同步整个数据库，请将 tableList 设置为 ".*".
                .tableList("gmall_config.t_user") // 设置捕获的表
                .username("root")
                .password("000000")
                .deserializer(new JsonDebeziumDeserializationSchema()) // 将 SourceRecord 转换为 JSON 字符串
                .build();

        // 设置 3s 的 checkpoint 间隔
        env.enableCheckpointing(3000);

        DataStreamSource<String> ds1 = env.fromSource(mySqlSource, WatermarkStrategy.noWatermarks(), "MySQL Source");
        ds1.print();
        env.execute();
    }
}
