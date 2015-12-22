
package main
/**
 * @author terry
 */

import org.joda.time.DateTime
import org.apache.spark.{Logging, SparkContext, SparkConf}  
import org.apache.spark.SparkContext._

object Main extends App with Logging{
  val conf = new SparkConf().setAppName(this.getClass.getName)
  lazy val sc = new SparkContext(conf)  
  case class WebsiteLog(rowKey:String="",
                 sessionId:String="",
                 ipAddress:String="",
                 timestamp:String="",
                 loadTimestamp:String="",
                 userId:String="",
                 url:String=""){
    override def toString=rowKey+","+sessionId+","+ipAddress
  }
 
  val file=this.args(0);
  
  //read from text file
  val input=sc.textFile(file)
  
  //parse the csv file and put records into a case class
  val data=input.filter(_.split(",").length==7)
        .filter(_!="rowKey,sessionId,ipAddress,timestamp,loadTimestamp,userId,url")
        .map(i=>{
          val fields=i.split(",")
          WebsiteLog(fields(0),fields(1),fields(2),fields(3),fields(4),fields(5),fields(6))   
        })
  
  //get the most recent login date for each userId      
  val recent=data.groupBy { x => x.userId }.map(i=>{
     val maxTime=i._2.maxBy { x =>  DateTime.parse(x.timestamp).getMillis}
     (i._1,maxTime.timestamp)
  })
  
  //get the count of unique login session for each userId 
  val frequent=data.groupBy { x => x.userId }.map(i=>{
     val count=i._2.map(_.sessionId).toSet.size
     (i._1,count)
  });
  
  //get the duration of unique login session for each userId
  val session=data.groupBy { x => x.userId+":"+x.sessionId }.map(i=>{
     val max=i._2.maxBy { x =>  DateTime.parse(x.timestamp).getMillis}
     val min=i._2.minBy { x =>  DateTime.parse(x.timestamp).getMillis}
     (min.userId,
         DateTime.parse(max.timestamp).getMillis
           -DateTime.parse(min.timestamp).getMillis)
  })
  
  //get the sum of unique login session duration for each userId
  val duration=session.groupBy{i=>i._1}.map(x=>{
    val total=x._2.map(_._2).reduce(_+_)
    (x._1,total)
  })

  case class RFM(userId:String,recency:String,frequency:Int,duration:Long){
    override def toString=userId+","+recency+","+frequency+","+duration
  }  
  
  // Join the data by userId and sort accordingly to create the RFM metric
 val rfm=recent.join(frequent).join(duration).map(i=>
   RFM(i._1,i._2._1._1,i._2._1._2,i._2._2)
 ).sortBy(x=>(x.recency,x.frequency,x.duration),false)

 //print results 
 rfm.collect().foreach {println}
 
}