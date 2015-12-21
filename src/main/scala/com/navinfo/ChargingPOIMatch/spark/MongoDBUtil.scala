package com.navinfo.ChargingPOIMatch.spark

import com.mongodb.BasicDBObject
import com.mongodb.MongoClient
import com.mongodb.MongoClientURI
import com.mongodb.client.FindIterable
import com.mongodb.client.MongoCollection
import com.mongodb.client.MongoDatabase
import com.mongodb.client.result.DeleteResult
import com.mongodb.client.result.UpdateResult
import com.mongodb.BasicDBObjectBuilder

/**
 * @author fangshaowei
 */
class MongoDBUtil {}

object MongoDBUtil {
  
  val mongoClient = {
    val sURI = "mongodb://%s:%s@%s:%d/%s"
    val uri = new MongoClientURI(sURI.format("dbadmin", "navinfo", "192.168.3.224", 27017, ""))
    new MongoClient(uri)
  }

  def getMongoDB(dbName: String): MongoDatabase = mongoClient.getDatabase(dbName)

  def getMongoDBTable(dbName: String, tableName: String): MongoCollection[BasicDBObject] = getMongoDB(dbName: String).getCollection(tableName, classOf[BasicDBObject])
  
  def search(query: BasicDBObject, table: MongoCollection[BasicDBObject]): FindIterable[BasicDBObject] = table.find(query)
  
  def getNearGeo(x: Double, y: Double, maxDis: Long, table: MongoCollection[BasicDBObject]): FindIterable[BasicDBObject] = {
    val query = new BasicDBObject("loc", new BasicDBObject("$near", 
                      new BasicDBObject("$geometry",
                          new BasicDBObject("type", "Point").append("coordinates", Array(x, y)))
                  .append("$maxDistance", maxDis)
                  .append("$spherical", true)))
    table.find(query)
  }
  
  def insert(insert: BasicDBObject, table: MongoCollection[BasicDBObject]) = table.insertOne(insert)
   
  def update(query: BasicDBObject, updateBson: BasicDBObject, table: MongoCollection[BasicDBObject]): UpdateResult = {
    val temp = new BasicDBObject
    temp.put("$set", updateBson)
    table.updateMany(query, temp)
  }
  
  def delete(delete: BasicDBObject, table: MongoCollection[BasicDBObject]): DeleteResult = table.deleteOne(delete)

  def main(args: Array[String]): Unit = {
    
    val table = getMongoDBTable("chargingPOI_match", "poi_dynamic")

    val query = new BasicDBObject("sockerParams.factory_num", "1101150019101")//1101150019104
    
    val start = System.currentTimeMillis()
    
//    for(i <- 0 until 5000){
//    	val updateItem = new BasicDBObject("sockerParams.$.sockerState", new Integer(4999 - i))
//    	val result = update(query, updateItem, table)
//    }
    
//    val updateItem = new BasicDBObject("sockerParams.$.sockerState", new Integer(111))
    val updateItem = new BasicDBObject("sockerParams.$.acableNum", new Integer(111))
//    updateItem.put("sockerParams.$.charge_fee", new Integer(222))
//    updateItem.put("sockerParams.$.serve_fee", new Integer(333))
    
//    updateItem.put("socker_num.sockableall_num", new Integer(999)//search(query, table).iterator().next().get("socker_num")
//    .append("sDCquickable_num", null)
//    .append("sDCslowable_num", null)
//    .append("sDCquick_num", 0)
//    .append("sDCslow_num", 0)
//    .append("sACslow_num", 0)
//    .append("sACquickable_num", null)
//    .append("sACslowable_num", null)
//    .append("sACquick_num", 0)
//    .append("sockerall_num", null)
//    )
    
    val result = update(query, updateItem, table)
    
    println(result)
    println((System.currentTimeMillis() - start).toString())
    
    val iter = search(query, table).iterator()
    
    while(iter.hasNext())
      println(iter.next())
    
//    val iter = getNearGeo(116.352982,39.89957, 1000, table).iterator()
  }
  
}