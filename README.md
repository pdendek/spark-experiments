spark-intro
===========

This is a sample Spark app. To run, type that on your cluster:
  
    git clone https://github.com/pdendek/spark-intro.git
    cd spark-intro  
    mvn clean install
    ./submit.sh SimpleApp target/spark-intro-1.0-SNAPSHOT-jar-with-dependencies.jar "file:///etc/passwd" 

