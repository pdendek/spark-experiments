package pl.adalab.spark

import util.control.Breaks._
import collection.JavaConversions._
import scala.io.Source
import Array._

/*** LocalFormatData.scala ***/

object LocalFormatData {
  def main(args: Array[String]) {
    val file = args(0)
    val input_data = Source.fromFile(file).mkString.split("\n").map(_.split("\t"));

    val data0 = unpackData(0,input_data(0).slice(1, input_data(0).length))
    val idxOfPersonId = getPersonIdIdx("EX_PERSON_ID", data0);
    val header = mixDataInCorrectOrder("ID",idxOfPersonId,data0); 
    val output_data = input_data.map(data => mixDataInCorrectOrder(data(0),idxOfPersonId,unpackData(1,data.slice(1, data.length))))
    
    val ret : Array[String] = Array[String](header) ++ output_data
    ret.foreach(println(_))
  }

  def mixDataInCorrectOrder(id : String,personIdx : Int, data : List[String]) : String = {
    var headerSB: StringBuilder = new StringBuilder(id+"\t");
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
  
  def getPersonIdIdx(name: String, in: List[String]): Int = {
    for ((x, i) <- in.view.zipWithIndex) if (name.equalsIgnoreCase(x)) return i;
    return -1;
  }

  def unpackData(idx: Int, input: Array[String]): List[String] = {
    val i: Int = input.length / 3
    var ret = List[String]()
    var j = 0;
    while (j < input.size) {
      ret = ret ::: List[String](input(j + idx))
      j += 3
    }
    return ret
  }
}
