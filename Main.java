package home_work1210;

import org.springframework.context.annotation.AnnotationConfigApplicationContext;

public class Main {
    public static void main(String[] args) {
        AnnotationConfigApplicationContext context = new AnnotationConfigApplicationContext(SparkConfig.class);
        JudgementService judgementService = context.getBean(JudgementService.class);
        System.out.println("the 3 most popular = " + judgementService.topX("ketty", 3));
        System.out.println("the 3 most popular = " + judgementService.commonPopularWords("britney","ketty", 3));
    }
}
