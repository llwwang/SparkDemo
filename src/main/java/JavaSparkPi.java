import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;

import java.util.ArrayList;
import java.util.List;

/**
 * Computes an approximation to pi Usage: JavaSparkPi [slices]
 * 数学原理，根据随机选择XY为-1到1的点落在半径为1的圆内的概率
 * http://stackoverflow.com/questions/34892522/the-principle-of-spark-pi
 */
public final class JavaSparkPi {

    public static void main(String[] args) throws Exception {
        SparkConf sparkConf = new SparkConf().setAppName("JavaSparkPi");
        JavaSparkContext jsc = new JavaSparkContext(sparkConf);

        int slices = (args.length == 1) ? Integer.parseInt(args[0]) : 200;
        int n = 100 * slices;//200个点
        List<Integer> l = new ArrayList<Integer>(n);
        for (int i = 0; i < n; i++) {
            l.add(i);
        }

        JavaRDD<Integer> dataSet = jsc.parallelize(l, slices);

        int count = dataSet.map(new Function<Integer, Integer>() {//Function是spark自己的类，含义是前一个Integer是输入类型，后一个Integer是返回类型

            public Integer call(Integer integer) {//每个dataSet的元素调用一次，一共被调用了200次
                double x = Math.random() * 2 - 1;
                double y = Math.random() * 2 - 1;
                return (x * x + y * y < 1) ? 1 : 0;//每次流向reduce层的确实只有1个数
            }
        }).reduce(new Function2<Integer, Integer, Integer>() {//Function2是spark自己的类，含义是前两个Integer是输入类型，最后一个Integer是返回类型

            public Integer call(Integer integer, Integer integer2) {//integer是上所有之前两两求和的总和，integer2是下一个map输出的结果
                System.out.println(integer + " " + integer2);
                /** 打印出两套差不多这个东西
                 *  0 1
                 1 1
                 2 1
                 3 0
                 3 1
                 4 0
                 4 1
                 5 1
                 ...
                 可见上面即加起来的过程
                 然后又打印出
                 75 76
                 可见是两个slices所有的0和1加起来，然后两个slices的和再相加
                 */
                return integer + integer2;
            }
        });

        System.out.println("Pi is roughly " + 4.0 * count / n);//这个是根据圆和正方形的面积

        jsc.stop();
    }
}