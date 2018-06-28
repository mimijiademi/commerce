import java.util.{Date, UUID}

import commons.conf.ConfigurationManager
import commons.constant.Constants
import commons.model.{UserInfo, UserVisitAction}
import commons.utils._
import net.sf.json.JSONObject
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SaveMode, SparkSession}

import scala.collection.mutable
import scala.util.Random

/**
  * Created by MiYang on 2018/6/25 9:30.
  */
object SessionStat {


  def main(args: Array[String]): Unit = {
    // 首先，读取我们的任务限制条件
    val jsonStr = ConfigurationManager.config.getString(Constants.TASK_PARAMS)
    val taskParm = JSONObject.fromObject(jsonStr)
    // 创建UUID,每个过程的创建都会生成独一无二的UUID，在数据库中作为主键，目的是区分每个任务的统计结果
    val taskUUID = UUID.randomUUID().toString
    // 创建sparkConf
    val sparkConf = new SparkConf().setAppName("sessionStat").setMaster("local[*]")
    // 创建sparkSession，同时制定hive可用
    val sparkSession = SparkSession.builder().config(sparkConf).enableHiveSupport().getOrCreate()
    // 获取动作表里的初始数据
    //UserVisitAction(2018-06-25,94,57967e9a3e10479480011a62c50a80b2,3,2018-06-25 5:06:48,重庆辣子鸡,-1,-1,null,null,null,null,9)
    val actionRDD = getBasicActionData(sparkSession, taskParm)

    // 转化为K-V结构，以sessionId为key，以actionRDD的每一行Row即UserVisitAction为value
    val sessionId2ActionRDD = actionRDD.map(item => (item.session_id, item))

    // （sessionId， iterable_action[UserVisitAction]）
    val sessionId2GroupRDD = sessionId2ActionRDD.groupByKey()
    //以后会多次用到这个数据，所以cache
    sessionId2GroupRDD.cache()

    val sessionId2FullInfoRDD = getSessionFullInfo(sparkSession, sessionId2GroupRDD)
    //(ea5f978bb73542d8924bf5ecae9b806a,
    // sessionid=ea5f978bb73542d8924bf5ecae9b806a|searchKeywords=重庆小面,日本料理,重庆辣子鸡,火锅,呷哺呷哺,温泉|clickCategoryIds=77,59,67,74,39,33,30,41|visitLength=3301|stepLength=33|startTime=2018-06-25 13:00:57|age=50|professional=professional70|sex=male|city=city39)

    //    sessionId2FullInfoRDD.foreach(println(_))

    //创建自定义累加器
    val sessionStatisticAccumulator = new SessionStatAccumulator
    //注册自定义累加器
    sparkSession.sparkContext.register(sessionStatisticAccumulator)

    // 第一：对数据进行过滤
    // 第二：对累加器进行累加
    val sessionId2FilteredRDD = getFilterRDD(sparkSession, taskParm, sessionStatisticAccumulator, sessionId2FullInfoRDD)

    sessionId2FilteredRDD.foreach(println(_))

    // 需求一：各范围session占比统计
    getSessionRatio(sparkSession, taskUUID, sessionStatisticAccumulator.value)

    // 需求二：随机抽取session
    // sessionId2FullInfoRDD：（sessionId, fullInfo） 一个session只有一条数据
    sessionRandomExtract(sparkSession, taskUUID, sessionId2FullInfoRDD)

    // sessionId2ActionRDD: 转化为K-V结构的原始行为数据 （sessionId, UserVisitAciton）
    // sessionId2FilteredRDD: 符合过滤条件的聚合数据  (sessionId, fullInfo)
    // 得到符合过滤条件的action数据
    val sessionId2FilteredActionRDD = sessionId2ActionRDD.join(sessionId2FilteredRDD).map {
      case (sessionId, (action, fullInfo)) =>
        (sessionId, action)
    }

    // 需求三：Top10热门品类统计
    val top10Category = getTop10PopularCategories(sparkSession, taskUUID, sessionId2FilteredActionRDD)

    // 需求四：统计每一个Top10热门品类的Top10活跃session
    // top10Category: Array -> (sortKey, fullInfo（cid|clickCount|OrderCount\payCOunt）)
    // sessionId2FilteredActionRDD: (sessionid, action)
    getTop10ActiveSession(sparkSession, taskUUID, top10Category, sessionId2FilteredActionRDD)

  }

  def getTop10ActiveSession(sparkSession: SparkSession,
                            taskUUID: String,
                            top10Category: Array[(SortedKey, String)],
                            sessionId2FilteredActionRDD: RDD[(String, UserVisitAction)]): Unit = {
    // top10Category样例类，sparkSession.sparkContext.makeRDD(top10Category) -> RDD[(sortKey, fullInfo)]
    // top10CategoryRDD: (cid, cid)得到top10品类RDD
    val top10CategoryRDD = sparkSession.sparkContext.makeRDD(top10Category).map {
      case (sortKey, fullInfo) =>
        val cid = StringUtils.getFieldFromConcatString(fullInfo, "\\|", Constants.FIELD_CATEGORY_ID).toLong
        (cid, cid)
    }
    // 符合条件的数据sessionId2FilteredActionRDD : (sid, aciton)   ==> (cid, action)
    val cid2ActionRDD = sessionId2FilteredActionRDD.map {
      case (sid, action) =>
        val cid = action.click_category_id
        (cid, action)
    }
    //第一步：点击过top10品类的所有session （sessionId,action）
    // 先cid2ActionRDD.join(top10CategoryRDD)已经只剩下点击过Top10品类的所有action数据
    // 再map: 进行格式转换
    val sessionId2FilteredRDD = cid2ActionRDD.join(top10CategoryRDD).map {
      case (cid, (action, categoryId)) =>
        val sessionid = action.session_id
        (sessionid, action)
    }
    // 第二步：统计每个session对Top10热门品类的点击次数
    // sessionId2FilteredRDD: (sid, action)
    // sessionId2GroupRDD: (sid,iterableAction)
    val sessionId2GroupRDD = sessionId2FilteredRDD.groupByKey()

    val cid2SessionCountRDD = sessionId2GroupRDD.flatMap {
      case (sessionId, iterableAction) =>
        //    categoryCountMap ： HashMap =>  [Long,Long]  第一个long代表categoryId,
        //    第二个long代表当前的session对于当前品类categoryId的点击次数 count
        val categoryCountMap = new mutable.HashMap[Long, Long]()

        // 经过以下循环之后，我们的categoryCountMap就被填充完毕，里面记录了
        // 当前的session对于Top10热门品类中每一个品类的点击次数
        for (action <- iterableAction) {
          val categoryId = action.click_category_id
          if (!categoryCountMap.contains(categoryId))
            categoryCountMap += (categoryId -> 0)
          categoryCountMap.update(categoryId, categoryCountMap(categoryId) + 1)
        }
        //yield作用：将传进来的集合中的每一条数据做一定的逻辑处理，再以整体集合（与传进来的集合类型相同）返回
        //yield返回的数据：map(（cid1,sessionId1=count1）,（cid1,sessionId2=count2）,（cid2,sessionId2=count2）,...)
        for ((cid, count) <- categoryCountMap)
          yield (cid, sessionId + "=" + count)
    }
    // cid2GroupSessionCountRDD : (cid, iterable[sessionId1=count1, sessionid2=count2,.....])
    val cid2GroupSessionCountRDD = cid2SessionCountRDD.groupByKey()

    val top10SessionRDD = cid2GroupSessionCountRDD.flatMap{
      // iterableSessionCount是一个iterable类型，里面是String，每一个String都是sessionid=count
      case(cid,iterableSessionCount)=>
        //sortedSession  : 降序排好序的(sessionId=count)
        val sortedSession = iterableSessionCount.toList.sortWith((item1,item2)=>
        item1.split("=")(1).toLong > item2.split("=")(1).toLong)

        val top10Session = sortedSession.take(10)

        //将top10数据放到样例类中，以便写到MySQL
        val top10SessionCaseClass = top10Session.map{
          item =>
            val sessionid = item.split("=")(0)
            val count = item.split("=")(1).toLong
            Top10Session(taskUUID,cid,sessionid,count)
        }
        top10SessionCaseClass
    }
    //将样例类数据写到MySQL
    import sparkSession.implicits._
    top10SessionRDD.toDF().write
      .format("jdbc")
      .option("url", ConfigurationManager.config.getString(Constants.JDBC_URL))
      .option("dbtable", "top10_session0115")
      .option("user", ConfigurationManager.config.getString(Constants.JDBC_USER))
      .option("password", ConfigurationManager.config.getString(Constants.JDBC_PASSWORD))
      .mode(SaveMode.Append)
      .save()
    }

  def getTop10PopularCategories(sparkSession: SparkSession,
                                taskUUID: String,
                                sessionId2FilteredActionRDD: RDD[(String, UserVisitAction)]) = {
    // 首先得到所有被点击、下单、付款过的品类
    // categoryId2CidRDD : (cid , cid)
    var categoryId2CidRDD = sessionId2FilteredActionRDD.flatMap {
      case (sessionId, action) =>
        val categoryArray = new mutable.ArrayBuffer[(Long, Long)]()

        if (action.click_category_id != -1L) {
          categoryArray += ((action.click_category_id, action.click_category_id))
        } else if (action.order_category_ids != null) {
          for (cid <- action.order_category_ids.split(",")) {
            categoryArray += ((cid.toLong, cid.toLong))
          }
        } else if (action.pay_category_ids != null) {
          for (cid <- action.pay_category_ids.split(",")) {
            categoryArray += ((cid.toLong, cid.toLong))
          }
        }
        categoryArray
    }
    //所以被点击、下单、付款的品类（不重复）
    //（cid,cid）不重复
    categoryId2CidRDD = categoryId2CidRDD.distinct()
    // 统计每一个被点击的品类的点击次数
    // （cid, count）
    val clickCount = getCategoryClickCount(sessionId2FilteredActionRDD)

    // 统计每一个被下单的品类的下单次数
    val orderCount = getCategoryOrderCount(sessionId2FilteredActionRDD)

    // 统计每一个被付款的品类的付款次数
    val payCount = getCategoryPayCount(sessionId2FilteredActionRDD)

    // (cid, fullInfo(cid|clickCount|orderCount|payCount))
    val cid2FullInfo = getFullInfoCount(categoryId2CidRDD, clickCount, orderCount, payCount)

    val sortKey2FullInfo = cid2FullInfo.map {
      case (cid, fullInfo) =>
        val clickCount = StringUtils.getFieldFromConcatString(fullInfo, "\\|", Constants.FIELD_CLICK_COUNT).toLong
        val orderCount = StringUtils.getFieldFromConcatString(fullInfo, "\\|", Constants.FIELD_ORDER_COUNT).toLong
        val payCount = StringUtils.getFieldFromConcatString(fullInfo, "\\|", Constants.FIELD_PAY_COUNT).toLong

        val sortKey = new SortedKey(clickCount, orderCount, payCount)
        (sortKey, fullInfo)
    }
    // (sortKey, fullInfo)
    val top10Category = sortKey2FullInfo.sortByKey(false).take(10)
    val top10CategoryRDD = sparkSession.sparkContext.makeRDD(top10Category)

    val top10CategoryWriteRDD = top10CategoryRDD.map {
      case (sortKey, fullInfo) =>
        val categoryId = StringUtils.getFieldFromConcatString(fullInfo, "\\|", Constants.FIELD_CATEGORY_ID).toLong
        val clickCount = sortKey.clickCount
        val orderCount = sortKey.orderCount
        val payCount = sortKey.payCount
        //Top10Category是定义的样例类
        Top10Category(taskUUID, categoryId, clickCount, orderCount, payCount)
    }

    import sparkSession.implicits._
    top10CategoryWriteRDD.toDF().write
      .format("jdbc")
      .option("url", ConfigurationManager.config.getString(Constants.JDBC_URL))
      .option("user", ConfigurationManager.config.getString(Constants.JDBC_USER))
      .option("password", ConfigurationManager.config.getString(Constants.JDBC_PASSWORD))
      .option("dbtable", "top10_category0115")
      .mode(SaveMode.Append)
      .save()

    top10Category
  }

  def getFullInfoCount(categoryId2CidRDD: RDD[(Long, Long)],
                       clickCount: RDD[(Long, Long)],
                       orderCount: RDD[(Long, Long)],
                       payCount: RDD[(Long, Long)]): RDD[(Long, String)] = {
    // (cid, (cid, clickCount)
    val clickCountRDD = categoryId2CidRDD.leftOuterJoin(clickCount).map {
      case (categoryId, (cid, cCount)) =>
        val count = if (cCount.isDefined) cCount.get else 0L
        val aggrInfo = Constants.FIELD_CATEGORY_ID + "=" + categoryId + "|" + Constants.FIELD_CLICK_COUNT + "=" + count
        (categoryId, aggrInfo)
    }
    val orderCountRDD = clickCountRDD.leftOuterJoin(orderCount).map {
      case (cid, (aggrInfo, oCount)) =>
        val count = if (oCount.isDefined) oCount.get else 0L
        val orderInfo = aggrInfo + "|" + Constants.FIELD_ORDER_COUNT + "=" + count
        (cid, orderInfo)
    }

    val payCountRDD = orderCountRDD.leftOuterJoin(payCount).map {
      case (cid, (aggrInfo, pCount)) =>
        val count = if (pCount.isDefined) pCount.get else 0L
        val payInfo = aggrInfo + "|" + Constants.FIELD_PAY_COUNT + "=" + count
        (cid, payInfo)
    }
    payCountRDD
  }

  def getCategoryPayCount(sessionId2FilteredActionRDD: RDD[(String, UserVisitAction)]): RDD[(Long, Long)] = {
    // 将所有对应于付款行为的action过滤出来
    val sessionIdFilterRDD = sessionId2FilteredActionRDD.filter {
      case (sessionId, action) => action.pay_category_ids != null
    }
    val cid2Num = sessionIdFilterRDD.flatMap {
      case (sessionId, action) => action.pay_category_ids.split(",").map(item => (item.toLong, 1L))
    }
    cid2Num.reduceByKey(_ + _)
  }

  def getCategoryOrderCount(sessionId2FilteredActionRDD: RDD[(String, UserVisitAction)]): RDD[(Long, Long)] = {
    // 将所有对应于下单行为的action过滤出来
    val sessionIdFilterRDD = sessionId2FilteredActionRDD.filter {
      case (sessionId, action) => action.order_category_ids != null
    }

    val cid2Num = sessionIdFilterRDD.flatMap {
      case (sessionId, action) => action.order_category_ids.split(",").map(item => (item.toLong, 1L))
    }
    cid2Num.reduceByKey(_ + _)
  }

  def getCategoryClickCount(sessionId2FilteredActionRDD: RDD[(String, UserVisitAction)]): RDD[(Long, Long)] = {
    // sessionId2FilteredActionRDD: (sessionId, action)
    // 过滤掉所有没有发生过点击的aciton
    val sessionId2FilterRDD = sessionId2FilteredActionRDD.filter {
      case (sessionId, action) =>
        action.click_category_id != -1
    }

    //将其map成（点击类别，1）即（click_category_id,1）
    val cid2NumRDD = sessionId2FilterRDD.map {
      case (sessionId, action) => (action.click_category_id, 1L)
    }
    //按照点击类别聚合 即（click_category_id,n）
    cid2NumRDD.reduceByKey(_ + _)
  }

  def sessionRandomExtract(sparkSession: SparkSession,
                           taskUUID: String,
                           sessionId2FullInfoRDD: RDD[(String, String)]): Unit = {
    val dateHour2FullInfo = sessionId2FullInfoRDD.map {
      case (sessionId, fullInfo) =>
        val startTime = StringUtils.getFieldFromConcatString(fullInfo, "\\|", Constants.FIELD_START_TIME)
        // 得到yyyy-MM-dd_HH
        val dateHour = DateUtils.getDateHour(startTime)

        (dateHour, fullInfo)

    }
    // (dateHour, fullInfo)
    // countByKey返回一个Map，记录了每个小时的session个数 (dateHour, count)
    val dateHourCountMap = dateHour2FullInfo.countByKey()

    // 嵌套Map结构：第一层key为date，第二层key为hour,value为count
    val dateHourSessionCountMap = new mutable.HashMap[String, mutable.HashMap[String, Long]]()
    for ((dateHour, count) <- dateHourCountMap) {
      val date = dateHour.split("_")(0)
      val hour = dateHour.split("_")(1)

      dateHourSessionCountMap.get(date) match {
        case None => dateHourSessionCountMap(date) = new mutable.HashMap[String, Long]()
          dateHourSessionCountMap(date) += (hour -> count)
        case Some(map) => dateHourSessionCountMap(date) += (hour -> count)
      }
    }

    for ((date, hourCountMap) <- dateHourSessionCountMap) {
      println("*******************************************")
      println("date: " + date)
      for ((hour, count) <- hourCountMap) {
        println("hour:" + hour + "   count" + count)
        println("------------------------------------------")
      }
      println("*******************************************")
    }

    println()
    println()



    //得到每天要抽取的session数量
    val sessionExtractCountPerDay = 100 / dateHourSessionCountMap.size
    // (date, (hour, List(index1, index2, index3, ...)))
    val dateHourRandomIndexMap = new mutable.HashMap[String, mutable.HashMap[String, mutable.ListBuffer[Int]]]()

    val random = new Random()
    //得到每个小时要抽取session的对应的随机索引们
    def generateRandomIndex(sessionExtractCountPerDay: Long, //每天要抽取的session个数
                            sessionDayCount: Long, //每天的session个数
                            hourCountMap: mutable.HashMap[String, Long],
                            hourIndexMap: mutable.HashMap[String, mutable.ListBuffer[Int]]): Unit = {
      // 得到这一个小时要抽取多少个session
      for ((hour, count) <- hourCountMap) {
        var hourExtractSessionCount = ((count / sessionDayCount.toDouble) * sessionExtractCountPerDay).toInt
        //限制最大取到每个小时的count数
        if (hourExtractSessionCount > count.toInt) {
          hourExtractSessionCount = count.toInt
        }

        hourIndexMap.get(hour) match {
          //第一次get后边什么都没有，即没有索引的list，所以生成一个
          case None => hourIndexMap(hour) = new mutable.ListBuffer[Int]
            for (i <- 0 until hourExtractSessionCount) {
              //生成随机数，nextInt代表生成Int类型的，count.toInt代表生成的随机数在0~count中
              var index = random.nextInt(count.toInt)
              //去除重复的随机index，如果一样，重新生成，直到不一样
              while (hourIndexMap(hour).contains(index)) {
                index = random.nextInt(count.toInt)
              }
              //将生成的随机索引加到索引的LIst中
              hourIndexMap(hour) += index
            }

          case Some(list) =>
            for (i <- 0 until hourExtractSessionCount) {
              //生成随机数，nextInt代表生成Int类型的，count.toInt代表生成的随机数在0~count中
              var index = random.nextInt(count.toInt)
              //去除重复的随机index，如果一样，重新生成，直到不一样
              while (hourIndexMap(hour).contains(index)) {
                index = random.nextInt(count.toInt)
              }
              //将生成的随机索引加到索引的LIst中
              hourIndexMap(hour) += index
            }
        }
      }
    }


    for ((date, hourCountMap) <- dateHourSessionCountMap) {
      //得到一天的session总数
      val dayCount = hourCountMap.values.sum

      dateHourRandomIndexMap.get(date) match {
        case None => dateHourRandomIndexMap(date) = new mutable.HashMap[String, mutable.ListBuffer[Int]]()
          generateRandomIndex(sessionExtractCountPerDay, dayCount, hourCountMap, dateHourRandomIndexMap(date))
        case Some(map) =>
          generateRandomIndex(sessionExtractCountPerDay, dayCount, hourCountMap, dateHourRandomIndexMap(date))
      }
    }


    for ((date, hourCountMap) <- dateHourRandomIndexMap) {
      println("*******************************************")
      println("date: " + date)
      for ((hour, list) <- hourCountMap) {
        println("hour:" + hour)
        for (item <- list) {
          print(item + ", ")
        }
        println()
        println("------------------------------------------")
      }
      println("*******************************************")
    }


    // 我们知道了每个小时要抽取多少条session，并且生成了对应随机索引的List
    // （dateHour, fullInfo）


    //将符合要求的fullInfo聚合成Iterable类型的，即IterableFullInfo
    val dateHour2GroupRDD = dateHour2FullInfo.groupByKey()


    val extractSessionRDD = dateHour2GroupRDD.flatMap {
      case (dateHour, iterableFullInfo) =>
        val date = dateHour.split("_")(0)
        val hour = dateHour.split("_")(1)

        //得到List（index1,index2,...）
        val indexList = dateHourRandomIndexMap.get(date).get(hour)

        //SessionRandomExtract是定义好的样例类,声明样例类，用于保存抽取的数据
        val extractSessionArray = new mutable.ArrayBuffer[SessionRandomExtract]()

        //将IterableFullInfo中的每一条fullInfo对应给一个索引，从0到100【因为要抽取100 条数据】
        var index = 0
        //将iterable类型的fullInfo遍历，将与随机生成的索引对应的值拿出来，放到样例类SessionRandomExtract中
        for (fullInfo <- iterableFullInfo) {
          if (indexList.contains(index)) {
            val sessionId = StringUtils.getFieldFromConcatString(fullInfo, "\\|", Constants.FIELD_SESSION_ID)
            val startTime = StringUtils.getFieldFromConcatString(fullInfo, "\\|", Constants.FIELD_START_TIME)
            val searchKeywords = StringUtils.getFieldFromConcatString(fullInfo, "\\|", Constants.FIELD_SEARCH_KEYWORDS)
            val clickCategories = StringUtils.getFieldFromConcatString(fullInfo, "\\|", Constants.FIELD_CLICK_CATEGORY_IDS)
            //将符合条件的每一条数据都加到样例类中去
            extractSessionArray += SessionRandomExtract(taskUUID, sessionId, startTime, searchKeywords, clickCategories)

          }
          //索引加1
          index += 1
        }
        //将样例类返回
        extractSessionArray
    }

    //将对应索引处的值组成的RDD存到MySQL中去
    import sparkSession.implicits._
    extractSessionRDD.toDF().write
      .format("jdbc")
      .option("url", ConfigurationManager.config.getString(Constants.JDBC_URL))
      .option("user", ConfigurationManager.config.getString(Constants.JDBC_USER))
      .option("password", ConfigurationManager.config.getString(Constants.JDBC_PASSWORD))
      .option("dbtable", "session_random_extract_0115")
      .mode(SaveMode.Append)
      .save()
  }

  def getSessionRatio(sparkSession: SparkSession,
                      taskUUID: String,
                      value: mutable.HashMap[String, Int]): Unit = {
    val session_count = value.getOrElse(Constants.SESSION_COUNT, 1).toDouble

    val visit_length_1s_3s = value.getOrElse(Constants.TIME_PERIOD_1s_3s, 0)
    val visit_length_4s_6s = value.getOrElse(Constants.TIME_PERIOD_4s_6s, 0)
    val visit_length_7s_9s = value.getOrElse(Constants.TIME_PERIOD_7s_9s, 0)
    val visit_length_10s_30s = value.getOrElse(Constants.TIME_PERIOD_10s_30s, 0)
    val visit_length_30s_60s = value.getOrElse(Constants.TIME_PERIOD_30s_60s, 0)
    val visit_length_1m_3m = value.getOrElse(Constants.TIME_PERIOD_1m_3m, 0)
    val visit_length_3m_10m = value.getOrElse(Constants.TIME_PERIOD_3m_10m, 0)
    val visit_length_10m_30m = value.getOrElse(Constants.TIME_PERIOD_10m_30m, 0)
    val visit_length_30m = value.getOrElse(Constants.TIME_PERIOD_30m, 0)

    val step_length_1_3 = value.getOrElse(Constants.STEP_PERIOD_1_3, 0)
    val step_length_4_6 = value.getOrElse(Constants.STEP_PERIOD_4_6, 0)
    val step_length_7_9 = value.getOrElse(Constants.STEP_PERIOD_7_9, 0)
    val step_length_10_30 = value.getOrElse(Constants.STEP_PERIOD_10_30, 0)
    val step_length_30_60 = value.getOrElse(Constants.STEP_PERIOD_30_60, 0)
    val step_length_60 = value.getOrElse(Constants.STEP_PERIOD_60, 0)

    //2代表保留两位小数
    val visit_length_1s_3s_ratio = NumberUtils.formatDouble(visit_length_1s_3s / session_count, 2)
    val visit_length_4s_6s_ratio = NumberUtils.formatDouble(visit_length_4s_6s / session_count, 2)
    val visit_length_7s_9s_ratio = NumberUtils.formatDouble(visit_length_7s_9s / session_count, 2)
    val visit_length_10s_30s_ratio = NumberUtils.formatDouble(visit_length_10s_30s / session_count, 2)
    val visit_length_30s_60s_ratio = NumberUtils.formatDouble(visit_length_30s_60s / session_count, 2)
    val visit_length_1m_3m_ratio = NumberUtils.formatDouble(visit_length_1m_3m / session_count, 2)
    val visit_length_3m_10m_ratio = NumberUtils.formatDouble(visit_length_3m_10m / session_count, 2)
    val visit_length_10m_30m_ratio = NumberUtils.formatDouble(visit_length_10m_30m / session_count, 2)
    val visit_length_30m_ratio = NumberUtils.formatDouble(visit_length_30m / session_count, 2)

    val step_length_1_3_ratio = NumberUtils.formatDouble(step_length_1_3 / session_count, 2)
    val step_length_4_6_ratio = NumberUtils.formatDouble(step_length_4_6 / session_count, 2)
    val step_length_7_9_ratio = NumberUtils.formatDouble(step_length_7_9 / session_count, 2)
    val step_length_10_30_ratio = NumberUtils.formatDouble(step_length_10_30 / session_count, 2)
    val step_length_30_60_ratio = NumberUtils.formatDouble(step_length_30_60 / session_count, 2)
    val step_length_60_ratio = NumberUtils.formatDouble(step_length_60 / session_count, 2)

    val stat = SessionAggrStat(taskUUID, session_count.toInt, visit_length_1s_3s_ratio, visit_length_4s_6s_ratio, visit_length_7s_9s_ratio,
      visit_length_10s_30s_ratio, visit_length_30s_60s_ratio, visit_length_1m_3m_ratio,
      visit_length_3m_10m_ratio, visit_length_10m_30m_ratio, visit_length_30m_ratio,
      step_length_1_3_ratio, step_length_4_6_ratio, step_length_7_9_ratio,
      step_length_10_30_ratio, step_length_30_60_ratio, step_length_60_ratio)


    val statRDD = sparkSession.sparkContext.makeRDD(Array(stat))
    import sparkSession.implicits._
    statRDD.toDF().write
      .format("jdbc")
      .option("url", ConfigurationManager.config.getString(Constants.JDBC_URL))
      .option("user", ConfigurationManager.config.getString(Constants.JDBC_USER))
      .option("password", ConfigurationManager.config.getString(Constants.JDBC_PASSWORD))
      .option("dbtable", "session_stat_0115")
      .mode(SaveMode.Append)
      .save()
  }

  def getFilterRDD(sparkSession: SparkSession,
                   taskParm: JSONObject,
                   sessionStatisticAccumulator: SessionStatAccumulator,
                   sessionId2FullInfoRDD: RDD[(String, String)]) = {
    val startAge = ParamUtils.getParam(taskParm, Constants.PARAM_START_AGE)
    val endAge = ParamUtils.getParam(taskParm, Constants.PARAM_END_AGE)
    val professionals = ParamUtils.getParam(taskParm, Constants.PARAM_PROFESSIONALS)
    val cities = ParamUtils.getParam(taskParm, Constants.PARAM_CITIES)
    val sex = ParamUtils.getParam(taskParm, Constants.PARAM_SEX)
    val searchKeywords = ParamUtils.getParam(taskParm, Constants.PARAM_KEYWORDS)
    val clickCategories = ParamUtils.getParam(taskParm, Constants.PARAM_CATEGORY_IDS)

    var filterInfo = (if (startAge != null) Constants.PARAM_START_AGE + "=" + startAge + "|" else "") +
      (if (endAge != null) Constants.PARAM_END_AGE + "=" + endAge + "|" else "") +
      (if (professionals != null) Constants.PARAM_PROFESSIONALS + "=" + professionals + "|" else "") +
      (if (cities != null) Constants.PARAM_CITIES + "=" + cities + "|" else "") +
      (if (sex != null) Constants.PARAM_SEX + "=" + sex + "|" else "") +
      (if (searchKeywords != null) Constants.PARAM_KEYWORDS + "=" + searchKeywords + "|" else "") +
      (if (clickCategories != null) Constants.PARAM_CATEGORY_IDS + "=" + clickCategories + "|" else "")

    if (filterInfo.endsWith("\\|")) {
      filterInfo = filterInfo.substring(0, filterInfo.length - 1)
    }

    //进行过滤操作
    val sessionId2FilteredRDD = sessionId2FullInfoRDD.filter {
      case (sessionId, fullInfo) =>
        var success = true

        if (!ValidUtils.between(fullInfo, Constants.FIELD_AGE, filterInfo, Constants.PARAM_START_AGE, Constants.PARAM_END_AGE)) {
          success = false
        }
        if (!ValidUtils.in(fullInfo, Constants.FIELD_PROFESSIONAL, filterInfo, Constants.PARAM_PROFESSIONALS)) {
          success = false
        }
        if (!ValidUtils.in(fullInfo, Constants.FIELD_CITY, filterInfo, Constants.PARAM_CITIES)) {
          success = false
        }
        if (!ValidUtils.equal(fullInfo, Constants.FIELD_SEX, filterInfo, Constants.PARAM_SEX)) {
          success = false
        }
        if (!ValidUtils.in(fullInfo, Constants.FIELD_SEARCH_KEYWORDS, filterInfo, Constants.PARAM_KEYWORDS)) {
          success = false
        }
        if (!ValidUtils.in(fullInfo, Constants.FIELD_CATEGORY_ID, filterInfo, Constants.PARAM_CATEGORY_IDS)) {
          success = false
        }

        if (success) {
          sessionStatisticAccumulator.add(Constants.SESSION_COUNT)

          def calculateVisitLength(visitLength: Long) {
            if (visitLength >= 1 && visitLength <= 3) {
              sessionStatisticAccumulator.add(Constants.TIME_PERIOD_1s_3s)
            } else if (visitLength >= 4 && visitLength <= 6) {
              sessionStatisticAccumulator.add(Constants.TIME_PERIOD_4s_6s)
            } else if (visitLength >= 7 && visitLength <= 9) {
              sessionStatisticAccumulator.add(Constants.TIME_PERIOD_7s_9s)
            } else if (visitLength >= 10 && visitLength <= 30) {
              sessionStatisticAccumulator.add(Constants.TIME_PERIOD_10s_30s)
            } else if (visitLength > 30 && visitLength <= 60) {
              sessionStatisticAccumulator.add(Constants.TIME_PERIOD_30s_60s)
            } else if (visitLength > 60 && visitLength <= 180) {
              sessionStatisticAccumulator.add(Constants.TIME_PERIOD_1m_3m)
            } else if (visitLength > 180 && visitLength <= 600) {
              sessionStatisticAccumulator.add(Constants.TIME_PERIOD_3m_10m)
            } else if (visitLength > 600 && visitLength <= 1800) {
              sessionStatisticAccumulator.add(Constants.TIME_PERIOD_10m_30m)
            } else if (visitLength > 1800) {
              sessionStatisticAccumulator.add(Constants.TIME_PERIOD_30m)
            }
          }


          def calculateStepLength(stepLength: Long): Unit = {
            if (stepLength >= 1 && stepLength <= 3) {
              sessionStatisticAccumulator.add(Constants.STEP_PERIOD_1_3)
            } else if (stepLength >= 4 && stepLength <= 6) {
              sessionStatisticAccumulator.add(Constants.STEP_PERIOD_4_6)
            } else if (stepLength >= 7 && stepLength <= 9) {
              sessionStatisticAccumulator.add(Constants.STEP_PERIOD_7_9)
            } else if (stepLength >= 10 && stepLength <= 30) {
              sessionStatisticAccumulator.add(Constants.STEP_PERIOD_10_30)
            } else if (stepLength > 30 && stepLength <= 60) {
              sessionStatisticAccumulator.add(Constants.STEP_PERIOD_30_60)
            } else if (stepLength > 60) {
              sessionStatisticAccumulator.add(Constants.STEP_PERIOD_60)
            }
          }

          val stepLength = StringUtils.getFieldFromConcatString(fullInfo, "\\|", Constants.FIELD_STEP_LENGTH).toLong
          val visitLength = StringUtils.getFieldFromConcatString(fullInfo, "\\|", Constants.FIELD_VISIT_LENGTH).toLong

          calculateStepLength(stepLength)
          calculateVisitLength(visitLength)

        }
        success
    }
    sessionId2FilteredRDD
  }


  def getSessionFullInfo(sparkSession: SparkSession,
                         sessionId2GroupRDD: RDD[(String, Iterable[UserVisitAction])]) = {
    val userId2AggrInfoRDD = sessionId2GroupRDD.map {
      case (sessionId, iterableAction) =>
        var startTime: Date = null
        var endTime: Date = null
        val searchKeywords = new StringBuffer("")
        val clickCategories = new StringBuffer("")
        var userId = -1L
        var stepLength = 0L

        for (action <- iterableAction) {
          if (userId == -1L) {
            userId = action.user_id
          }

          // 更新起始时间和结束时间
          val actionTime = DateUtils.parseTime(action.action_time)
          if (startTime == null || startTime.after(actionTime))
            startTime = actionTime
          if (endTime == null || endTime.before(actionTime))
            endTime = actionTime

          // 完成搜索关键词的追加（去重）
          val searkKeyWord = action.search_keyword
          if (StringUtils.isNotEmpty(searkKeyWord) && !searchKeywords.toString.contains(searkKeyWord))
            searchKeywords.append(searkKeyWord + ",")

          // 完成点击品类的追加（去重）
          val clickCategory = action.click_category_id
          if (clickCategory != -1L && !clickCategories.toString.contains(clickCategory))
            clickCategories.append(clickCategory + ",")

          stepLength += 1

        }

        //trimComma()去除最后的逗号
        val searchKW = StringUtils.trimComma(searchKeywords.toString)
        val clickCG = StringUtils.trimComma(clickCategories.toString)
        // 获取访问时长(s)
        val visitLength = (endTime.getTime - startTime.getTime) / 1000
        // aggrInfo 字段名=字段值|字段名=字段值|  到开始时间
        val aggrInfo = Constants.FIELD_SESSION_ID + "=" + sessionId + "|" +
          Constants.FIELD_SEARCH_KEYWORDS + "=" + searchKW + "|" +
          Constants.FIELD_CLICK_CATEGORY_IDS + "=" + clickCG + "|" +
          Constants.FIELD_VISIT_LENGTH + "=" + visitLength + "|" +
          Constants.FIELD_STEP_LENGTH + "=" + stepLength + "|" +
          Constants.FIELD_START_TIME + "=" + DateUtils.formatTime(startTime)

        (userId, aggrInfo)
    }

    val sql = "select * from user_info"
    import sparkSession.implicits._
    val userId2UserInfoRDD = sparkSession.sql(sql).as[UserInfo].rdd.map(item => (item.user_id, item))

    // userId2AggrInfoRDD -> (userId, aggrInfo)
    // userId2UserInfo -> (userId, UserInfo)
    val sessionId2FullInfoRDD = userId2AggrInfoRDD.join(userId2UserInfoRDD).map {
      case (userId, (aggrInfo, userInfo)) =>
        val fullInfo = aggrInfo + "|" + Constants.FIELD_AGE + "=" + userInfo.age + "|" +
          Constants.FIELD_PROFESSIONAL + "=" + userInfo.professional + "|" +
          Constants.FIELD_SEX + "=" + userInfo.sex + "|" +
          Constants.FIELD_CITY + "=" + userInfo.city

        // userInfo 字段名=字段值|字段名=字段值| 用户的ID、名称 、名字 、年龄、职业、城市、 性别
        val sessionId = StringUtils.getFieldFromConcatString(aggrInfo, "\\|", Constants.FIELD_SESSION_ID)
        (sessionId, fullInfo)
    }
    sessionId2FullInfoRDD
  }


  def getBasicActionData(sparkSession: SparkSession, taskParm: JSONObject) = {
    val startDate = ParamUtils.getParam(taskParm, Constants.PARAM_START_DATE)
    val endDate = ParamUtils.getParam(taskParm, Constants.PARAM_END_DATE)

    val sql = "select * from user_visit_action where date>='" + startDate + "'and date<='" + endDate + "'"
    import sparkSession.implicits._
    sparkSession.sql(sql).as[UserVisitAction].rdd
  }


}
