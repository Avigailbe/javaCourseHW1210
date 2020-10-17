package home_work1210;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;

/**
 * @author Evgeny Borisov
 */
@Configuration
@ComponentScan
public class SparkConfig {

    @Bean
    public SparkConf devSparkConf() {

        return new SparkConf().setAppName("demo").setMaster("local[*]");
    }

    @Bean
    public JavaSparkContext sc(SparkConf sparkConf) {
        return new JavaSparkContext(sparkConf);
    }

}



