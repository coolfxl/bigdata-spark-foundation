package com.ibeifeng.sparkproject.studyspark;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;

import scala.Tuple2;

public class TransformationOperations {

	public static void main(String[] args) {
//		map();
//		filter();
//		flatMap();
//		groupByKey();
//		reduceByKey();
//		sortByKey();
		joinAndCogroup();
	}
	
	//将集合中的数字乘以2
	private static void map(){
		SparkConf conf = new SparkConf()
		.setAppName("map")
		.setMaster("local");
		
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		//构造集合
		List<Integer> numbers = Arrays.asList(1,2,3,4,5);
		
		//并行化集合，创建初始RDD
		JavaRDD<Integer> numberRDD = sc.parallelize(numbers);
		
		/**
		 * 使用map算子将集合中的每个元素都乘以2
		 * map算子是任何类型的RDD都可以调用的，
		 * java中，map算子接收的参数是function对象
		 * 创建的function对象一定要设置第二个泛型参数
		 * 这个泛型类型，就是返回的新元素的类型
		 * 同时call()方法的返回类型，也必须与第二个泛型类型同同步
		 * call()方法内部，就可以对原始RDD中国的每一个元素进行各种处理和计算，
		 * 并返回一个新的元素，所有的新的元素就会组成一个的RDD
		 */
		JavaRDD<Integer> number2RDD = numberRDD.map(new Function<Integer, Integer>(){

			private static final long serialVersionUID = 1L;
			
			/**
			 * 传入call()方法的就是1,2,3,4,5
			 */
			@Override
			public Integer call(Integer v1) throws Exception {
				return v1 * 2;
			}
		});
		
		//打印新的RDD
		number2RDD.foreach(new VoidFunction<Integer>(){

			private static final long serialVersionUID = 1L;
			/**
			 * 在这里1-10都会传进来，
			 * 但是根据我们的逻辑，只有2,4,6,8,10会返回true
			 * 所以只有偶数会保留下来，放在新的RDD中
			 */
			@Override
			public void call(Integer t) throws Exception {
				System.out.println(t);
			}
		});
		
		
		sc.close();
	}
	
	//过滤集合中的偶数
	public static void filter(){
		SparkConf conf = new SparkConf()
		.setAppName("filter")
		.setMaster("local");
		
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		//构造集合
		List<Integer> numbers = Arrays.asList(1,2,3,4,5,6,7,8,9,10);
		
		//并行化集合，创建初始RDD
		JavaRDD<Integer> numberRDD = sc.parallelize(numbers);	
		
		/**
		 * 对初始RDD执行filter算子，过滤其中的偶数
		 * filter算子传入的也是function，其他的使用注意点，实际上和map是一样的
		 * 但是唯一不同的是，就是call()方法返回的类型是boolean
		 * 每一个初始RDD中的元素，都会传入call()方法，此时你可以执行各种自定义的计算逻辑
		 * 来判断这个元素是否是你想要的
		 * 如果你想在新的RDD中保留这个，那么就返回true，否则，不想保留这个元素，返回false
		 * 
		 */
		JavaRDD<Integer> evenRdd = numberRDD.filter(new Function<Integer, Boolean>(){

			private static final long serialVersionUID = 1L;

			@Override
			public Boolean call(Integer v1) throws Exception {
				return v1 % 2 == 0;
			}
			
		});	
		evenRdd.foreach(new VoidFunction<Integer>(){

			private static final long serialVersionUID = 1L;

			@Override
			public void call(Integer t) throws Exception {
				System.out.println(t);
			}
			
		});
		
		sc.close();
	}
	
	//将文本行拆分为单词 
	public static void flatMap(){
		SparkConf conf = new SparkConf()
		.setAppName("flatMap")
		.setMaster("local");
		
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		//构造集合
		List<String> numbers = Arrays.asList("hello you","hello me","hello world");
		
		//并行化集合，创建初始RDD
		JavaRDD<String> numberRDD = sc.parallelize(numbers);	
		
		/**
		 * flatMap算子在java中接收的参数是FlatMapFunction
		 * 我们需要定义FlatMapFunction的第二个泛型类型，也就是代表了返回的新元素的类型
		 * call()方法，返回的类型，不是U，而是Iterator<U> ，这里的U也与第二个泛型类型相同
		 * flatMap其实就是，就收原始的RDD中的每个元素，并进行各种逻辑的计算和处理，返回可以返回多个元素
		 * 多个元素，级封装在iterable集合中，可以使用ArrayList等集合
		 * 新的RDD中，封装了所有的新元素，也就是锁，新的RDD的大小，一定是>=原始的RDD
		 */
		JavaRDD<String> evenRdd = numberRDD.flatMap(new FlatMapFunction<String,String>(){

			private static final long serialVersionUID = 1L;
			/**
			 * 这里传入的hello you
			 * 返回的是iterable<String>(hello, you)
			 */
			@Override
			public Iterable<String> call(String t) throws Exception {
				return Arrays.asList(t.split(" "));
			}
		});	
		evenRdd.foreach(new VoidFunction<String>(){

			private static final long serialVersionUID = 1L;

			@Override
			public void call(String t) throws Exception {
				System.out.println(t);
			}
			
		});
		
		sc.close();
	}
	
	//将每个班级的成绩进行分组
	public static void groupByKey(){
		SparkConf conf = new SparkConf()
		.setAppName("groupByKey")
		.setMaster("local");
		
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		//构造集合
		@SuppressWarnings(value={"unchecked"})
		List<Tuple2<String,Integer>> scoreList = Arrays.asList(
				new Tuple2<String,Integer>("class1",70),
				new Tuple2<String,Integer>("class2",89),
				new Tuple2<String,Integer>("class1",80),
				new Tuple2<String,Integer>("class2",90));
		
		//并行化集合，创建初始RDD
		JavaPairRDD<String, Integer> scores = sc.parallelizePairs(scoreList);	
		
		/**
		 * 针对scores RDD,执行groupByKey算子，对每个班级的成绩进行分组
		 * groupByKey算子，返回的还是javaPairRDD
		 * 但是javaPairRDD的第一个泛型类型不变，第二个泛型类型变成iterable这种集合类型
		 * 也及时说，按照了key进行分组，那么每个key可能都会有多个value，此时value聚合成了iterable
		 * 那么接下来，只要同伙groupedScores这种JavaPairRdd，可以方便的处理数据
		 */
		JavaPairRDD<String, Iterable<Integer>> groupedScores = scores.groupByKey();
		groupedScores.foreach(new VoidFunction<Tuple2<String, Iterable<Integer>>>(){

			private static final long serialVersionUID = 1L;

			@Override
			public void call(Tuple2<String, Iterable<Integer>> t)
					throws Exception {
				System.out.println(t._1);
				Iterator<Integer> ite = t._2.iterator();
				while(ite.hasNext()){
					System.out.println(ite.next());
				}
				System.out.println("=====");
			}
		});
		
		sc.close();
	}
	
	//统计每个班级的总分
	public static void reduceByKey(){
		SparkConf conf = new SparkConf()
		.setAppName("reduceByKey")
		.setMaster("local");
		
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		//构造集合
		@SuppressWarnings(value={"unchecked"})
		List<Tuple2<String,Integer>> scoreList = Arrays.asList(
				new Tuple2<String,Integer>("class1",70),
				new Tuple2<String,Integer>("class2",60),
				new Tuple2<String,Integer>("class1",80),
				new Tuple2<String,Integer>("class2",90));
		
		//并行化集合，创建初始RDD
		JavaPairRDD<String, Integer> scores = sc.parallelizePairs(scoreList);	
		
		/**
		 * 累加班级分数
		 * reduceByKey接收的参数是functino2类型，他有三个泛型，实际上代表了3个值
		 * 第一个泛型和第二个泛型类型，代表了原始RDD中的元素的value的类型
		 * 因此对每个key进行reduce，都会依次将第一个、第二个value传入，将值再与第三个value传入
		 * 因此此处，会自动定义两个泛型，代表call()方法的两个传入参数的类型
		 * 第三个泛型类型，代表了每次reduce操作返回的值的类型，默认也是与原始的RDD的value类型相同的
		 * reduceByKey算法返回的RDD，还是javapairRDD<key,value>
		 */
		JavaPairRDD<String, Integer> totalScores = scores.reduceByKey(new Function2<Integer, Integer, Integer>(){

			private static final long serialVersionUID = 1L;

			@Override
			public Integer call(Integer v1, Integer v2) throws Exception {
				return v1+v2;
			}
		});
		
		
		totalScores.foreach(new VoidFunction<Tuple2<String, Integer>>(){

			private static final long serialVersionUID = 1L;
			/**
			 * 对每个key,都会将其value，一次传入call方法中
			 * 从而聚合出每个key对应的一个value
			 * 然后，将每个key对应的一个value,组合成一个tuple2，作为新的RDD元素
			 */
			@Override
			public void call(Tuple2<String, Integer> t) throws Exception {
				System.out.println(t._1+"==="+t._2);
			}
		});
		sc.close();
	}
	
	//按照学生成绩进行排序
	public static void sortByKey(){
		SparkConf conf = new SparkConf()
		.setAppName("sortByKey")
		.setMaster("local");
		
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		//构造集合
		@SuppressWarnings(value={"unchecked"})
		List<Tuple2<Integer,String>> scoreList = Arrays.asList(
				new Tuple2<Integer,String>(70,"leo"),
				new Tuple2<Integer,String>(50,"tom"),
				new Tuple2<Integer,String>(80,"marry"),
				new Tuple2<Integer,String>(60,"cool"));
		
		//并行化集合，创建初始RDD
		JavaPairRDD<Integer, String> scores = sc.parallelizePairs(scoreList);	
		
		/**
		 * sortByKey()其实就是根据key进行排序，可以手动指定顺序
		 * 返回的还是JavaPairRDD，其中元素的内容和原始的RDD一模一样，但是RDD的元素的顺序不同
		 */
		JavaPairRDD<Integer, String> sortedScores = scores.sortByKey(false);//默认升序排序，false添加后降序
		
		sortedScores.foreach(new VoidFunction<Tuple2<Integer, String>>(){

			private static final long serialVersionUID = 1L;

			@Override
			public void call(Tuple2<Integer, String> t) throws Exception {
				System.out.println(t._1+"=="+t._2);
			}
		});
		sc.close();
	}
	
	//打印学生成绩
	public static void joinAndCogroup(){
		SparkConf conf = new SparkConf()
		.setAppName("joinAndCogroup")
		.setMaster("local");
		
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		//构造集合
		@SuppressWarnings(value={"unchecked"})
		List<Tuple2<Integer, String>> studentList = Arrays.asList(
				new Tuple2<Integer, String>(1,"leo"),
				new Tuple2<Integer, String>(2,"jack"),
				new Tuple2<Integer, String>(3,"tom")
		);
		
		@SuppressWarnings("unchecked")
		List<Tuple2<Integer, Integer>> scoreList = Arrays.asList(
				new Tuple2<Integer, Integer>(1,100),
				new Tuple2<Integer, Integer>(2, 90),
				new Tuple2<Integer, Integer>(3, 60),
				new Tuple2<Integer, Integer>(1,10),
				new Tuple2<Integer, Integer>(2, 60)
		);
		
		//并行化两个集合，创建初始RDD
		JavaPairRDD<Integer, String> student = sc.parallelizePairs(studentList);
		JavaPairRDD<Integer, Integer> scores = sc.parallelizePairs(scoreList);	
		
		/**
		 * 使用join算子关联两个RDD
		 * 根据key进行join,返回JavaPairRDD
		 * 但是JavaPairRDD的第一个泛型类型，之前两个JavaPairRDD的key的类型，因为是通过key进行join的
		 * 第二个泛型类型，是tuple2<v1,v2>的类型，tuple2的连个泛型分别为原始RDD的value的类型
		 * join就返回RDD的每个元素，就是通过key join起来的pair
		 * 类似mysql的join操作
		 */
		JavaPairRDD<Integer, Tuple2<String, Integer>> studentScores = student.join(scores);
		
		studentScores.foreach(new VoidFunction<Tuple2<Integer, Tuple2<String, Integer>>>(){

			private static final long serialVersionUID = 1L;

			@Override
			public void call(Tuple2<Integer, Tuple2<String, Integer>> t)
					throws Exception {
				System.out.println(t._1+"==="+t._2._1+"=="+t._2._2);
				/**
				 * 	1===leo==100
					1===leo==10
					3===tom==60
					2===jack==90
					2===jack==60
				 */
			}
			
		});
		JavaPairRDD<Integer, Tuple2<Iterable<String>, Iterable<Integer>>> cogruopStudentScores = student.cogroup(scores);
		cogruopStudentScores.foreach(new VoidFunction<Tuple2<Integer,Tuple2<Iterable<String>,Iterable<Integer>>>>(){

			private static final long serialVersionUID = 1L;

			@Override
			public void call(
					Tuple2<Integer, Tuple2<Iterable<String>, Iterable<Integer>>> t)
					throws Exception {
				System.out.println(t._1+"==="+t._2._1+"===="+t._2._2);
				/**
				 * 	1===[leo]====[100, 10]
					3===[tom]====[60]
					2===[jack]====[90, 60]
				 */
			}
			
		});
		sc.close();
	}
	
	//RDD持久化
	public static void cache(){
		SparkConf conf = new SparkConf()
		.setAppName("WordCountLocal")
		.setMaster("local");
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		JavaRDD<String> lines = sc
				.textFile("C:\\Users\\Administrator\\Desktop\\test\\word.txt")
				.cache();
		long beginTime = System.currentTimeMillis();
		long count = lines.count();
		long endTime = System.currentTimeMillis();
		System.out.println(count);
		System.out.println("1:"+(endTime-beginTime));
		
		
		beginTime = System.currentTimeMillis();
		count = lines.count();
		endTime = System.currentTimeMillis();
		System.out.println(count);
		System.out.println("2:"+(endTime-beginTime));
		
		sc.close();
	}
	
	//统计单词个数
	public static void wordCount(){
		/**
		 * 第一步：创建sparkContext对象，设置spark应用的配置信息
		 * 使用setMaster()可以设置应用程序要连接的spark集群的master节点的url
		 * 但是如果设置为local则代表，在本地运行
		 * 
		 */
		SparkConf conf = new SparkConf()
		.setAppName("WordCountLocal")
		.setMaster("local");
		/**
		 * 第二部：创建javaSparkContext对象
		 * 在spark中，sparkcontext是spark所有功能的一个路口，你无论是用java， Scala甚至是Python编写，
		 * 都必须要有一个sparkcontext，他的主要作用，包括初始化spark应用程序所需的一些核心组件，包括
		 * 调度器（DAGScheduler TaskScheduler） 还会去spark master节点上进行注册
		 * 但是在spark中，编写不同类型的spark盈余公程序，使用的sparkcontext是不同的，如果使用功能Scala
		 * 使用的就是原生的sparkcontext对象
		 * java	就是javasparkcontext对象
		 * spark sql 就是sqlcontext，hivecontext
		 * spark streaming 就是它独有的sparkcontext
		 * 
		 */
		@SuppressWarnings("resource")
		JavaSparkContext sc = new JavaSparkContext(conf);
		/**
		 * 第三部：要针对输入员（hdfs文件，本地文件，等等）,创建一个初始的RDD
		 * 输入源中的数据会被大三，分配到RDD中的每个partition中，从而形成一个初始的分布式的数据集
		 * 因为是本地测试，这里只针对本地文件
		 * sparkcontext中，用于根据文件类型的输入源创建RDD的方法，叫做textfile()方法
		 * 在java中，创建的普通的RDD，则叫做javaRDD
		 * 在这里呢，RDD中有元素这种概念，如果是hdfs或者本地文件，创建的RDD，每一个元素就相当于文件里的一行
		 */
		JavaRDD<String> lines = sc.textFile("C:\\Users\\Administrator\\Desktop\\test\\word.txt");
		/**
		 * 第四步：对初始化的RDD进行transformation操作，也就是一些计算操作
		 * 通常操作会通过创建function，并配合RDD的map、flatmap等算子来执行function
		 * 通常，如果比较简单，则创建指定function的匿名内部类
		 * 如果function比较复杂，则会单独创建一个类，作为实现这个function接口的类
		 * 先将每一行拆分成单个的单词
		 */
		/**
		 * flatmapfunction有两个泛型参数，分别代表了输入和输出类型
		 * 我们这里是string，因为是一行一行的文本，输出，其实也是string，因为是每一行的文本
		 * 所以，flatmap的作用其实就是将RDD的一个元素，给拆分成一个或多个元素
		 * flatmap有点类似解压缩的操作
		 */
		JavaRDD<String> words = lines.flatMap(new FlatMapFunction<String,String>(){

			private static final long serialVersionUID = 1L;

			@Override
			public Iterable<String> call(String line) throws Exception {
				return Arrays.asList(line.split(" "));
			}
			
		});
		/**
		 * 接着，需要将一个单词，映射成为（单词，1）的这种格式
		 * 因为只有这样，后面才能根据单词作为key，来进行每个单词的出现次数的叠加
		 * maptopair其实就是将每个元素，映射成为一个（v1,v2）这样的tuple2类型的元素
		 * tuple2就是Scala类型，包含了两个值
		 * maptopair这个算子，要求的是与pairfunction配合使用，第一个泛型参数代表了输入类型
		 * 第二个和第三个泛型参数，代表的输出的tuple2的第一个值和第二个值的类型
		 * JavaPairRDD的连两个泛型分别代表了tuple元素的第一个值和第二个值的类型
		 */
		JavaPairRDD<String, Integer> pairs = words.mapToPair(
				new PairFunction<String,String, Integer>(){

					private static final long serialVersionUID = 1L;

					@Override
					public Tuple2<String, Integer> call(String word)
							throws Exception {
						
						return new Tuple2<String, Integer>(word, 1);
					}
			
		});
		/**
		 * 接着，需要以单词为key，统计每个单词出现的次数
		 * 这里要使用reducebykey这个算子，对每个key对应的value，都进行reduce操作
		 * 比如JavaPairRDD中有几个元素，分别为（hello，1） （hello，1）（hello，1）（word，1）
		 * reduce操作相当于是把第一值和第二个值进行计算，然后再将结果与第三个值进行计算，比如这里的hello，那么就是相当于
		 * 首先是1+1 =2，再将2+1 =3，也就是将所有的value进行累加操作
		 * 最后返回的JavaPairRDD中的元素，也是tuple，但是第一个值就是每个key，第二个值就是key的value
		 * reduce之后的结果，就相当于是每个单词出现的次数
		 */
		JavaPairRDD<String, Integer> wordCounts = pairs.reduceByKey(
				new Function2<Integer, Integer, Integer>(){

					private static final long serialVersionUID = 1L;

					@Override
					public Integer call(Integer v1, Integer v2)
							throws Exception {
						return v1 + v2;
					}
			
		});//到这里位置，已经统计出了单词的次数，上面的操作都是transformation操作，还要有action操作
		
		/**
		 * foreach触发action的执行
		 */
		wordCounts.foreach(new VoidFunction<Tuple2<String, Integer>>(){

			private static final long serialVersionUID = 1L;

			@Override
			public void call(Tuple2<String, Integer> t) throws Exception {
				System.out.println(t._1+"=="+t._2);//拿tuple的值是通过下划线来拿的
			}
			
		});
	}
	
	/**
	 * spark-submit的补充说说明
	 * 没有指定提交到哪个集群中去，用的就是本地local模式，只是模拟集群模式来运行
	 * 如果要提交到集群中运行，则要添加--master 集群地址 7077默认端口
	 */
}
