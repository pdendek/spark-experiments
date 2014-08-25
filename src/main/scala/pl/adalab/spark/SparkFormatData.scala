import util.control.Breaks._
import collection.JavaConversions._
import scala.io.Source
import Array._
import org.apache.spark.{ SparkConf, SparkContext }
import com.google.common.base.Splitter
import java.util.Collection
import org.apache.spark.rdd.RDD
import com.google.common.base.Joiner

/*** SparkFormatData.scala ***/

object SparkFormatData {

  def main(args: Array[String]) {
    val in_file = args(0)
    val out_file = args(1)
    val sc = new SparkContext(new SparkConf().setAppName("Spark Count"))
    val readed_file = sc.textFile(in_file, 2)
    val splited_file = readed_file.map(x => x.split("\t"))

    val printline: String = Joiner.on(",").join(splited_file.first().iterator)
    println(printline)

    val first_line: Array[String] = splited_file.first();
    val idxOfPersonId = MyFucntions.getPersonIdIdx("EX_PERSON_ID", first_line.slice(0, first_line.length));
    println(idxOfPersonId)
//    val header = MyFucntions.mixDataInCorrectOrder("ID", idxOfPersonId, first_line.slice(0, first_line.length));
////        println(header)
//
//    val formated_data = splited_file.
//    		map(x => MyFucntions.
//    		    mixDataInCorrectOrder(x(0), idxOfPersonId, MyFucntions.
//    		        unpackData(1, x.slice(1, x.length))
//    		))
//    formated_data.foreach(println(_))
////        val out_rdd = sc.parallelize(Array[String](header) ++ formated_data.collect())
////        out_rdd.saveAsTextFile(out_file)
  }

  object MyFucntions {

    def unpackData(idx: Int, input: Array[String]): Array[String] = {
      val i: Int = input.length / 3
      var ret = Array[String]()
      var j = 1;
      while (j < input.size) {
        ret = ret ++ Array[String](input(j + idx))
        j += 3
      }
      return ret
    }

    def mixDataInCorrectOrder(id: String, personIdx: Int, data: Array[String]): String = {
      var headerSB: StringBuilder = new StringBuilder(id + "\t");
      var i = 1
      while (i < data.length) {
        breakable {
          if (i == personIdx) {
            break;
          }
          headerSB.append(data(i) + "\t")
        }
        i += 1
      }
      headerSB.append(data(personIdx));
      return headerSB.toString()
    }

    def getPersonIdIdx(name: String, in: Array[String]): Int = {
      for ((x, i) <- in.view.zipWithIndex) if (name.equalsIgnoreCase(x)) return i;
      return -1;
    }
  }
}
