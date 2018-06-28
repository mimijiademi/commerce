import org.apache.spark.util.AccumulatorV2
import scala.collection.mutable

/**
  * Created by MiYang on 2018/6/25 14:27.
  * isZero: 当AccumulatorV2中存在类似数据不存在这种问题时，是否结束程序。
    copy: 拷贝一个新的AccumulatorV2
    reset: 重置AccumulatorV2中的数据
    add: 操作数据累加方法实现
    merge: 合并数据
    value: AccumulatorV2对外访问的数据结果
  */

class SessionStatAccumulator extends AccumulatorV2[String, mutable.HashMap[String, Int]]{

  val countMap = new mutable.HashMap[String, Int]()

  override def isZero: Boolean = countMap.isEmpty

  // Excutor会从driver中复制一个累加器到自己的内存中
  override def copy(): AccumulatorV2[String, mutable.HashMap[String, Int]] = {
    val acc = new SessionStatAccumulator
    acc.countMap ++= this.countMap	// ++=  是将两个map相加
    acc
  }

  override def reset(): Unit = countMap.clear()

  override def add(v: String): Unit = {
    if(!countMap.contains(v))//如果不包含传过来的String类型的值，即v【v作为key】,
      countMap += (v->0)//那么给v一个初始值0
    countMap.update(v, countMap(v) + 1)//将countMap中键值为v的对应的值加1
  }

  override def merge(other: AccumulatorV2[String, mutable.HashMap[String, Int]]): Unit = {
    other match {
      // acc.countMap.foldLeft(this.countMap) 等价于 this.countMap /: acc.countMap
      // (0 /: (1 to 100))(_+_)

      // foldLeft左折叠，相当于以下
      // map
      // for((k,v) <- acc.countMap){
      // map += (k -> (map.getOrElse(k, 0) + v))
      // }

      case acc:SessionStatAccumulator => acc.countMap.foldLeft(this.countMap){
        case (map, (k,v)) => map += (k -> (map.getOrElse(k, 0) + v))
      }
    }
  }

  override def value: mutable.HashMap[String, Int] = {
    this.countMap
  }
}
