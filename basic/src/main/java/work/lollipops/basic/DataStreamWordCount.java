package work.lollipops.basic;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * 数据流的处理方式
 *
 * @author zhaohaoren
 */
public class DataStreamWordCount {

    public static void main(String[] args) throws Exception {

        // 创建的环境=流处理环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 从socket中读取数据流   nc -lk 7777 => 输入word
        DataStream<String> inputDataStream = env.socketTextStream("localhost", 7777);
        DataStream<Tuple2<String, Integer>> wordCountDataStream = inputDataStream
                .flatMap(new DataSetWordCount.MyFlatMapper())
                .keyBy(0)
                .sum(1);

        wordCountDataStream.print()
                // 并行度，默认是cpu核心数，可以理解为多个线程处理
                .setParallelism(1);

        // 启动任务
        env.execute();
    }
}
