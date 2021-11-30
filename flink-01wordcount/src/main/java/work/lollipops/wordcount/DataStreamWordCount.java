package work.lollipops.wordcount;

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

        // 创建执行环境 （☆☆☆ 注意这里创建的是流环境 ☆☆☆）
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 从socket中读取数据流   nc -lk 7777 => 输入word
        DataStream<String> inputDataStream = env.socketTextStream("localhost", 7777);
        DataStream<Tuple2<String, Integer>> wordCountDataStream = inputDataStream
                // flatMap和DataSet的是通用的
                .flatMap(new DataSetWordCount.MyFlatMapper())
                // 流处理这里，没有groupBy了，而是keyBy对数据进行划分（因为数据是流的，单个是没有group概念的）
                // 数据依据key（key的hashcode）进入不同的分区
                .keyBy(0)
                .sum(1);

        wordCountDataStream.print()
                // 并行度，默认是cpu核心数，可以理解为多个线程处理
                .setParallelism(1);

        // 流处理任务和批处理任务不同，需要启动任务：作为流是一个任务一直跑着。上面只是构建了计算的拓扑print不是输出一次就完事了的。
        env.execute();
    }
}
