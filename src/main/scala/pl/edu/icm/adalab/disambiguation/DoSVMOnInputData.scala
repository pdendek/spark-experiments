package pl.edu.icm.adalab.disambiguation

import util.control.Breaks._
import collection.JavaConversions._
import Array._
import org.apache.spark.{ SparkConf, SparkContext }
import org.apache.spark.mllib.classification.SVMWithSGD
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.linalg.DenseVector
import org.apache.spark.rdd.RDD
import com.google.common.base.Joiner

/*** DoSVMOnInputData.scala ***/

object DoSVMOnInputData {

  def main(args: Array[String]) {
    println("All args:\t " + args.mkString(","))
    println("args.slice(0,2):\t" + args.slice(0, 2).mkString(","))
    println("args.slice(1,3):\t" + args.slice(1, 3).mkString(","))

    val sc = new SparkContext(new SparkConf().setAppName("Build Model: Load Data & Create Model"))

    transformData(Array[Any](args(0), args(1), sc))
    classifyWithSVM(Array[Any](args(1), args(2), sc))
  }

  def transformData(args: Array[Any]) {

    // Read Input Arguments
    val inFile		= 	args(0).asInstanceOf[String]
    val outFile 	= 	args(1).asInstanceOf[String]
    val sc 			= 	args(2).asInstanceOf[SparkContext]

    // Load data
    val readed_file = sc.textFile(inFile, 2)
    val splittedUnfiltered = readed_file.map(x => x.split("\t")).cache()
    val first_line0: Array[String] = splittedUnfiltered.first();
    val first_line = first_line0.slice(1, first_line0.length);
    val idxOfPersonId = MyFucntions.getPersonIdIdx("EX_PERSON_ID", MyFucntions.
      unpackData(0, first_line))
    val header = MyFucntions.mixDataInCorrectOrder("ID", idxOfPersonId, MyFucntions.unpackData(0, first_line))
    val formated_splittedFiltered = splittedUnfiltered.
      map(x => MyFucntions.
        mixDataInCorrectOrder(x(0), idxOfPersonId, MyFucntions.
          unpackData(2, x.slice(1, x.length)))).cache()
    val out_rdd = sc.parallelize(Array[String](header) ++ formated_splittedFiltered.collect())
    out_rdd.saveAsTextFile(outFile)
  }

  def classifyWithSVM(args: Array[Any]) {

    // Read Input Arguments
    val inFile		=	args(0).asInstanceOf[String]
    val outFile 	=	args(1).asInstanceOf[String]
    val sc 			=	args(2).asInstanceOf[SparkContext]
    
    // Load data
    val unsplittedFiltered = sc.textFile(inFile)
    val head = unsplittedFiltered.first.split("\t")
    val splittedUnfiltered = unsplittedFiltered.map(x => x.split("\t")).cache()
    val splittedFiltered = splittedUnfiltered.filter(a => {if(a(0).replaceAll("[0-9]", "").length()==0) (true) else (false)})

    // Get number of features, create initial vector of weights
    val featureNum = splittedFiltered.first.length - 2
    val initialWeights: DenseVector = new DenseVector(Seq.fill(featureNum)(1.0).toArray)

    // Parse input splittedFiltered
    val parsedData = splittedFiltered.map { columns : Array[String] =>
      val features: DenseVector = new DenseVector(columns.slice(1, columns.length-1).map(x => x.toDouble))
      LabeledPoint(columns(columns.length-1).toDouble, features)
    }

    // Classify
    val numIterations = 600
    val model = SVMWithSGD.train(parsedData, numIterations, 0.025, .1, 1.0, initialWeights)

    // Print Data About Model
    val modelHead 	=	 head.slice(1, head.length-1).mkString(" + ")
    val modelStr 	= 			model.weights.toArray.mkString(" + ")
    println("Model")
    println(modelHead+ " - " + "Intercept")
    println(modelStr + " - " + model.intercept)

    //Evaluate model on training examples and compute training error
    val labelAndPreds = parsedData.map { point =>
      val prediction = model.predict(point.features)
      (point.label, prediction)
    }
    val trainErr = labelAndPreds.filter(r => r._1 != r._2).count.toDouble / parsedData.count
    println("Training Error = " + trainErr)
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

    def mixDataInCorrectOrder(id: String, personIdx: Int, splittedFiltered: Array[String]): String = {
      var headerSB: StringBuilder = new StringBuilder(id + "\t");
      var i = 0
      while (i < splittedFiltered.length) {
        breakable {
          if (i == personIdx) {
            break;
          }
          headerSB.append(splittedFiltered(i) + "\t")
        }
        i += 1
      }
      headerSB.append(splittedFiltered(personIdx));
      return headerSB.toString()
    }

    def getPersonIdIdx(name: String, in: Array[String]): Int = {
      for ((x, i) <- in.view.zipWithIndex) if (name.equalsIgnoreCase(x)) return i;
      return -1;
    }
  }
}
