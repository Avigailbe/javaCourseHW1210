package home_work1210;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.storage.StorageLevel;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.PropertySource;
import org.springframework.stereotype.Component;
import org.springframework.stereotype.Service;
import scala.Tuple2;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;


@Component
@PropertySource("classpath:user.properties")
public class JudgementService implements Serializable {

    @Autowired
    private transient JavaSparkContext sc;// = SparkHolder.getSc();

    @Value("${garbage}")
    private List<String> garbageWords; //= ClassLoader.getSystemResource("user.properties");

    public List<String> topX(String artistName, int x) {

        JavaRDD<String> lines = sc.textFile("data/songs/" + artistName + "/*");

        JavaPairRDD<Integer,String>artist1Words = sortPopularWords(artistName);

        List<String>cleanWords = artist1Words
                .values()
                .collect();

        return cleanWords.stream().limit(x).collect(Collectors.toList());
    }

    public List<String> commonPopularWords(String artist1, String artist2, int x) {

        JavaPairRDD<Integer,String>artist1Words = sortPopularWords(artist1);
        JavaPairRDD<Integer,String>artist2Words = sortPopularWords(artist2);

        List<String>commonPopWords =artist1Words
                .join(artist2Words)
                .mapToPair(t1Tuple2Tuple2 -> t1Tuple2Tuple2._2())
                .sortByKey(false)
                .values()
                .collect();
        return commonPopWords.stream().limit(x).collect(Collectors.toList());
    }

    public JavaPairRDD<Integer,String> sortPopularWords(String artistName){

        JavaRDD<String> lines = sc.textFile("data/songs/" + artistName + "/*");

        JavaPairRDD<Integer,String>cleanWords = lines
                .map(String::toLowerCase)
                .flatMap(WordsUtil::getWords)
                .filter(word-> !this.garbageWords.contains(word))
                .mapToPair(w-> new Tuple2<>(1,w))
                .mapToPair(Tuple2::swap)
                .reduceByKey((s,l) -> (s+l))
                .mapToPair(Tuple2::swap)
                .sortByKey(false).persist(StorageLevel.MEMORY_AND_DISK());

        return cleanWords;
    }
}
