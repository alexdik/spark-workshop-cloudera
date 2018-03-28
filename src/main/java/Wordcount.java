import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;
import org.apache.spark.api.java.function.Function2;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Wordcount {
    private static final String IN = "/user/cloudera/wordcount/in";
    private static final String OUT = "/user/cloudera/wordcount/out";

    private static List<String> tokenize(String str) {
        Pattern pattern = Pattern.compile("\\w+");
        Matcher matcher = pattern.matcher(str);
        List<String> tokens = new ArrayList<>();
        while (matcher.find()) {
            tokens.add(matcher.group());
        }
        return tokens;
    }

    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("spark-workshop");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> rdd = sc.textFile(IN);

        JavaPairRDD<String, Integer> outputRdd = rdd
            .flatMap(new FlatMapFunction<String, String>() {
                public Iterable<String> call(String line) {
                    return tokenize(line);
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
