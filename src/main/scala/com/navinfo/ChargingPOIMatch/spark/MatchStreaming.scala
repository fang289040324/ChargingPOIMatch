package com.navinfo.ChargingPOIMatch.spark

import com.alibaba.fastjson.JSON
import com.mongodb.BasicDBObject
import com.navinfo.ChargingPOIMatch.mongodb.MongoDBUtil
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * @author fangshaowei
  */

class MatchStreaming {

  def startStreaming(zk: String, groupId: String, topics: String, numThreads: Int = 1): Unit = {

    val sparkConf = new SparkConf().setAppName("ChargingMatch")
    val ssc = new StreamingContext(sparkConf, Seconds(60))

    ssc.checkpoint("chargingMatch_checkpoint")

    val topicMap = topics.split(",").map((_, numThreads)).toMap
    val lines = KafkaUtils.createStream(ssc, zk, groupId, topicMap).map(_._2)

    //    val kafkaParams = Map[String, String]("metadata.broker.list" -> "xdatanode-18:9092",
    //                      "serializer.class" -> "kafka.serializer.StringEncoder")
    //    val topicSet = Set(topics)
    //    KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topicSet)

    println("==================== match start !!! ====================")
    println("======================= fetch lines count =========================")
    lines.count().foreachRDD(_.foreach(println))
    matchInfo(lines)
    ssc.start
    ssc.awaitTermination()
  }

  def matchInfo(lines: DStream[String]): Unit = {
    lines.foreachRDD(rdd => {
      rdd.map(JSON.parseObject).foreach(json => {

//        val sdf = new SimpleDateFormat("yyyyMMddhh:mm") // 线程不安全
//        val dateSdf = new SimpleDateFormat("yyyyMMdd")

        val price = json.getString("price")
        val staOpstateInt = staOpstateChange(json.getString("staOpstate"))

        val acableNum = json.getInteger("acableNum")
        val dcableNum = json.getInteger("dcableNum")

        val ableNum = (if (acableNum == null) 0 else acableNum.toInt) + (if (dcableNum == null) 0 else dcableNum.toInt)

        val priceList = json.getJSONArray("priceList")
        var chargeFee = "-1"
        var serveFee = "-1"
        var timeRange = ""
//        var timeRanges = new Array[String](2)
//        val currentTime = System.currentTimeMillis()
        for (i <- 0 until priceList.size()) {
          val priceJson = JSON.parseObject(String.valueOf(priceList.get(i)))
          timeRange += priceJson.getString("timeRange") + ","
          //          timeRanges = priceJson.getString("timeRange").split("~")
          //          val minTime = sdf.parse(dateSdf.format(currentTime) + timeRanges(0)).getTime
          //          val maxTime = sdf.parse(dateSdf.format(currentTime) + timeRanges(1)).getTime
          //          if (currentTime > minTime && currentTime <= maxTime) {
          chargeFee += priceJson.getString("electricPrice") + ","
          if (chargeFee == null || chargeFee.length() == 0) chargeFee = "-1"
          serveFee += priceJson.getString("servicePrice") + ","
          if (serveFee == null || serveFee.length() == 0) serveFee = "-1"
          //          }
        }
        val pileList = json.getJSONArray("pileList")
        for (i <- 0 until pileList.size()) {
          val pileJson = JSON.parseObject(String.valueOf(pileList.get(i)))
          val pileCode = pileJson.getString("pileCode")

          val state = pileStateChange(pileJson.getString("pileState"))

          val query = new BasicDBObject("sockerParams.factory_num", pileCode) //1101150019101
          val updateItem = new BasicDBObject
          updateItem.put("sockerParams.$.charge_fee", chargeFee.substring(0, chargeFee.length - 1))
          updateItem.put("sockerParams.$.serve_fee", serveFee.substring(0, serveFee.length - 1))
          updateItem.put("sockerParams.$.timeRange", timeRange.substring(0, timeRange.length - 1))
          updateItem.put("sockerParams.$.sockerState", new Integer(state))
          updateItem.put("socker_num.sockableall_num", new Integer(ableNum))
          updateItem.put("price", price)
          updateItem.put("state", new Integer(staOpstateInt))
          updateItem.put("socker_num.dcableNum", dcableNum)
          updateItem.put("socker_num.acableNum", acableNum)

          val result = MongoDBUtil.update(query, updateItem, MatchStreaming.table)

          if (result.getModifiedCount > 0) {
            println("==================" + pileCode)
            println("==================state: " + state + "==============pileState: " + pileJson.getString("pileState"))
//            println("==================timeRanges: " + timeRanges(0) + "-" + timeRanges(1))
            println("==================timeRanges: " + timeRange)
            println("==================chargeFee: " + chargeFee)
            println("==================serveFee: " + serveFee)
            println("==================ableNum: " + ableNum)
            println("==================price: " + price)
            println("==================state: " + staOpstateInt + "==============staOpstate: " + staOpstateInt)
            println("==================dcableNum: " + dcableNum)
            println("==================acableNum: " + acableNum)
            println("==================" + result)
            //            println("==================特来电：" + json)
            //            println("=======================================================================================================")
            //            println("==================四维：" + MongoDBUtil.search(query, MatchStreaming.table).first())
            //            println("=======================================================================================================")
          }
        }
      })
    })
  }

  private def pileStateChange(pileState: String): Int = {
    if (pileState == null || pileState.length() == 0) -1
    else if (pileState.equals("空闲")) 0
    else if (pileState.equals("已插枪") || pileState.equals("已充满") || pileState.equals("涓流充") || pileState.equals("充电中") || pileState.equals("暂停")) 1
    else if (pileState.equals("离网") || pileState.equals("故障")) 2
    else -1
  }

  private def staOpstateChange(staOpstate: String): Int = {
    if (staOpstate == null || staOpstate.length() == 0) -1
    else if (staOpstate.equals("初始") || staOpstate.equals("暂停营业")) 1
    else if (staOpstate.equals("运营中")) 0
    else if (staOpstate.equals("待运营")) 3
    else if (staOpstate.equals("关闭")) 5
    else -1
  }
}

object MatchStreaming {

  val mongoClient = MongoDBUtil.mongoClient

  val table = MongoDBUtil.getMongoDBTable("chargingPOI_match", "poi_dynamic")

  def main(args: Array[String]): Unit = {
    val ms = new MatchStreaming
    ms.startStreaming("192.168.4.196:2181/kafka", "test", "TLD")
  }

}