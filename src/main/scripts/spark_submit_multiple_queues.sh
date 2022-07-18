 export APPJAR=/home/vagrant/d/ideaprojects/SparkMulti/core/target/scala-2.13/sparkcore_2.13-0.1.jar

 spark-submit --class  core.SparkLogProcessorWithLogging \
--driver-java-options "-Dlog4j.configuration=file:///home/vagrant/d/ideaprojects/SparkMulti/src/main/resources/sparklogprocessor.log4j.yarn.properties -Dspark.yarn.app.container.log.dir=file:///home/vagrant" \
--conf spark.executor.extraJavaOptions=-Dlog4j.configuration=file:///home/vagrant/d/ideaprojects/SparkMulti/src/main/resources/sparklogprocessor.log4j.yarn.properties \
--master yarn \
--num-executors 1 \
--executor-memory 600M \
--deploy-mode cluster \
--queue q1 \
--conf spark.yarn.submit.waitAppCompletion=false \
$APPJAR apachelogs badrecsq1

spark-submit --class  core.SparkLogProcessorWithLogging \
--driver-java-options "-Dlog4j.configuration=file:///home/vagrant/d/ideaprojects/SparkMulti/src/main/resources/sparklogprocessor.log4j.yarn.properties -Dspark.yarn.app.container.log.dir=file:///home/vagrant" \
--conf spark.executor.extraJavaOptions=-Dlog4j.configuration=file:///home/vagrant/d/ideaprojects/SparkMulti/src/main/resources/sparklogprocessor.log4j.yarn.properties \
--master yarn \
--num-executors 1 \
--executor-memory 600M \
--deploy-mode cluster \
--queue q2 \
--conf spark.yarn.submit.waitAppCompletion=false \
$APPJAR apachelogs badrecsq2

spark-submit --class  core.SparkLogProcessorWithLogging \
--driver-java-options "-Dlog4j.configuration=file:///home/vagrant/d/ideaprojects/SparkMulti/src/main/resources/sparklogprocessor.log4j.yarn.properties -Dspark.yarn.app.container.log.dir=file:///home/vagrant" \
--conf spark.executor.extraJavaOptions=-Dlog4j.configuration=file:///home/vagrant/d/ideaprojects/SparkMulti/src/main/resources/sparklogprocessor.log4j.yarn.properties \
--master yarn \
--num-executors 1 \
--executor-memory 600M \
--deploy-mode cluster \
--queue q3 \
--conf spark.yarn.submit.waitAppCompletion=false \
$APPJAR apachelogs badrecsq3