package pl.edu.icm.adalab.disambiguation

import util.control.Breaks._
import collection.JavaConversions._
import Array._
import org.apache.spark.{ SparkConf, SparkContext }
import java.util.Date
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path

/*** SparkFormatData.scala ***/

object SparkFormatData {

  def main(args: Array[String]) {
    val in_file = args(0)
    val out_file = args(1)
    val sc = new SparkContext(new SparkConf().setAppName("Spark Count"))
    val readed_file = sc.textFile(in_file, 2)
    val splitted_file = readed_file.map(x => x.split("\t")).cache()
    val first_line: Array[String] = splitted_file.first();
    val idxOfPersonId = MyFucntions.getPersonIdIdx("EX_PERSON_ID", MyFucntions.
      unpackData(0, first_line.slice(1, first_line.length)))
    val header = MyFucntions.mixDataInCorrectOrder("ID", idxOfPersonId, MyFucntions.unpackData(0, first_line.slice(1, first_line.length)))
    val formated_data = splitted_file.
      map(x => MyFucntions.
        mixDataInCorrectOrder(x(0), idxOfPersonId, MyFucntions.
          unpackData(1, x.slice(1, x.length)))).cache()
    val out_rdd = sc.parallelize(Array[String](header) ++ formated_data.collect())
//    val fs : FileSystem = FileSystem.get(getConf());
//    fs.delete(new Path("path/to/file"), true); // delete file, true for recursive 
    out_rdd.saveAsTextFile(out_file) //+ "__" + new Date().toString().replaceAll("[\\s:]+", "_"))
  }

  object MyFucntions {

    def unpackData(idx: Int, input: Array[String]): Array[String] = {
      val i: Int = input.length / 3
      var ret = Array[String]()
      var j = 0;
      while (j < input.size) {
        ret = ret ++ Array[String](input(j + idx))
        j += 3
      }
      return ret
    }

    def mixDataInCorrectOrder(id: String, personIdx: Int, data: Array[String]): String = {
      var headerSB: StringBuilder = new StringBuilder(id + "\t");
      var i = 0
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
