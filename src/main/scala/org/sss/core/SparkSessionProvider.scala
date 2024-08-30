package org.sss.core

import org.apache.spark.sql.SparkSession

object SparkSessionProvider {

private val configs = Map(
  "spark.master" -> "local"
)
  /*
  "spark.master" -> "k8s://https://<your-k8s-api-server>:<port>",   // configuration for Kubernetes
  "spark.kubernetes.container.image" -> "<your-spark-image>",       // Docker image for Spark
  "spark.kubernetes.namespace" -> "<your-namespace>",               // Kubernetes namespace
  "spark.kubernetes.authenticate.driver.serviceAccountName" -> "<your-service-account>",  // Service account for driver
  "spark.executor.instances" -> "2",                                // Number of executors
  "spark.kubernetes.driver.pod.name" -> "<your-driver-pod-name>"    // Name of the driver pod
*/
  private val sparkBuilder = SparkSession.builder()
    .appName(System.getenv("APP_NAME"))

   configs.foreach { case (key, value) => sparkBuilder.config(key, value) }

  def getSparkSession: SparkSession = sparkBuilder.getOrCreate()
}
