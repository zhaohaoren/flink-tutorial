package work.lollipops.wordcount;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

/**
 * 使用的是DataSet方式，非流处理方式，而是数据集的方式（批处理方式，一般用于离线计算）
 *
 * @author zhaohaoren
 */
public class DataSetWordCount {

    public static final String FILE_PATH = "D:\\IdeaProjects\\flink-tutorial\\flink-01wordcount\\src\\main\\resources\\data_set.txt";

    public static void main(String[] args) throws Exception {
        // 创建执行环境
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSet<String> dataSet = env.readTextFile(FILE_PATH);
        // 按照word分组
        DataSet<Tuple2<String, Integer>> dataSetRes = dataSet
                .flatMap(new MyFlatMapper())
                .groupBy(0)
                // 对后面数字1求和，计数
                .sum(1);
        dataSetRes.print();
    }

    public static class MyFlatMapper implements FlatMapFunction<String, Tuple2<String, Integer>> {
        @Override
        public void flatMap(String value, Collector<Tuple2<String, Integer>> collector) {
            // value:读取的一行数据
            // collector:结果收集器
            String[] words = value.split(" ");
            for (String word : words) {
                collector.collect(new Tuple2<>(word, 1));
            }
        }
    }
}
