package com.bjsxt.study

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object TransformationOperations {
  def main(args: Array[String]): Unit = {
//    map()
//    filter()
//    flatMap()
//    groupByKey()
//    reduceByKey()
//    sortedByKey()
    joinAndCogroup()
    
  }
 
  //将集合中的数字乘以2
  def map(){
    val conf = new SparkConf().setAppName("map").setMaster("local")
    val sc = new SparkContext(conf)
    
    val numberarr = Array(1,2,3,4,5)
    val numbers = sc.parallelize(numberarr,1)
    val numbers2  = numbers.map { x => x * 2 }
    
    numbers2.foreach { line => println(line) }
  }
  
  //过滤集合中的偶数
  def filter(){
    val conf = new SparkConf().setAppName("filter").setMaster("local")
    val sc = new SparkContext(conf)
    
    val numberarr = Array(1,2,3,4,5)
    val numbers = sc.parallelize(numberarr,1)
    val numbers2  = numbers.filter{ numbers => numbers % 2 == 0}
    
    numbers2.foreach { line => println(line) }
  }
  
  //将文本行拆分为单词  
  def flatMap(){
    val conf = new SparkConf().setAppName("flatMap").setMaster("local")
    val sc = new SparkContext(conf)
    
    val string = Array("hello you","hello me","hello world");
    val numbers = sc.parallelize(string,1)
    val word = numbers.flatMap { line => line.split(" ")}
    
    word.foreach { line => println(line)}
  }
  
  //将每个班级的成绩进行分组
  def groupByKey(){
    val conf = new SparkConf().setAppName("groupByKey").setMaster("local")
    val sc = new SparkContext(conf)
    
    val scoreList = Array(Tuple2("class1",80),Tuple2("class2",70),Tuple2("class1",60),Tuple2("class2",90))
    val scores = sc.parallelize(scoreList, 1);
    val groupedScores  = scores.groupByKey()
    
    groupedScores.foreach(score => {
      println(score._1);
      score._2.foreach(single => {
          println(single)
        })
    })
  }
  
  //统计每个班级的总分
  def reduceByKey(){
    val conf = new SparkConf().setAppName("reduceByKey").setMaster("local")
    val sc = new SparkContext(conf)
    
    val scoreList = Array(Tuple2("class1",80),Tuple2("class2",70),Tuple2("class1",60),Tuple2("class2",90))
    val scores = sc.parallelize(scoreList, 1);
    val totalScores = scores.reduceByKey(_+_)
    
    totalScores.foreach( score => println(score._1+"==="+score._2))
  }
  
  //按照学生成绩进行排序
  def sortedByKey(){
    val conf = new SparkConf().setAppName("sortedByKey").setMaster("local")
    val sc = new SparkContext(conf)
    
    val scoreList = Array(Tuple2(70,"leo3"),Tuple2(50,"leo1"),Tuple2(80,"leo4"),Tuple2(60,"leo2"))
    val scores = sc.parallelize(scoreList, 1);//无论并行什么类型的集合都是用parallelize
    val totalScores = scores.sortByKey()
    
    totalScores.foreach( score => println(score._1+"==="+score._2))
  }
 
  //打印学生成绩
  def joinAndCogroup(){
    val conf = new SparkConf().setAppName("joinAndCogroup").setMaster("local")
    val sc = new SparkContext(conf)
    
    val studentList = Array(Tuple2(1,"leo3"),Tuple2(2,"leo1"),Tuple2(3,"leo4"),Tuple2(4,"leo2"))
    val scoreList = Array(Tuple2(1,100),Tuple2(2,60),Tuple2(3,70),Tuple2(4,64),Tuple2(4,65),Tuple2(3,90))
    
    val student = sc.parallelize(studentList, 1);
    val scores = sc.parallelize(scoreList, 1);//无论并行什么类型的集合都是用parallelize
    val studentScores = student.join(scores)
    
    studentScores.foreach(single => {
      println("sid"+single._1)
      println("sname"+single._2._1)
      println("score"+single._2._2)
      println("=======")
    })
  }
  
}