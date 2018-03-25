import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;
import org.apache.spark.api.java.function.Function2;

import java.util.Arrays;

public class Wordcount {
    private static final String IN = "/user/cloudera/wordcount/in";
    private static final String OUT = "/user/cloudera/wordcount/out";

    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("spark-workshop");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> rdd = sc.textFile(IN);

        JavaPairRDD<String, Integer> outputRdd = rdd
            .flatMap(new FlatMapFunction<String, String>() {
                public Iterable<String> call(String line) {
                    return Arrays.asList(line.split(" "));
                }
            })
            .mapToPair(new PairFunction<String, String, Integer>() {
                public Tuple2<String, Integer> call(String word) {
                    return new Tuple2<>(word, 1);
                }
            })
            .reduceByKey(new Function2<Integer, Integer, Integer>() {
                public Integer call(Integer v1, Integer v2) {
                    return v1 + v2;
                }
            });

        outputRdd.saveAsTextFile(OUT + "-" + System.currentTimeMillis());
    }
}
