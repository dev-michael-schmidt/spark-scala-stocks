package org.sss.core

import org.apache.spark.sql.SparkSession
import org.apache.log4j.Logger

object SparkSessionProvider {

  private val appName = Option(System.getenv("APP_NAME")).getOrElse("APP_NAME")
  private val logger = Logger.getLogger(getClass.getName)

  private val isKubernetes: Boolean = sys.env.getOrElse("RUN_MODE", "local") == "kubernetes"
  private val configs = if (isKubernetes) {
    Map(
      "spark.master" -> "k8s://https://<your-k8s-api-server>:<port>",   // configuration for Kubernetes
      "spark.kubernetes.container.image" -> "<your-spark-image>",       // Docker image for Spark
      "spark.kubernetes.namespace" -> "<your-namespace>",               // Kubernetes namespace
      "spark.kubernetes.authenticate.driver.serviceAccountName" -> "<your-service-account>",  // Service account for driver
      "spark.executor.instances" -> "2",                                // Number of executors
      "spark.kubernetes.driver.pod.name" -> "<your-driver-pod-name>"    // Name of the driver pod
    )
  } else {
    Map(
      "spark.master" -> "local"
    )
  }

  private val sparkBuilder = {
    if (appName == "APP_NAME") {
      logger.warn("appName has defaulted to `APP_NAME`, consider setting this env var")
    }

    val initialBuilder = SparkSession.builder().appName(appName)
    configs.foldLeft(initialBuilder) {
      case (builder, (key, value)) =>
        logger.info(s"Applying $key => $value")
        builder.config(key, value)
    }
  }

  // Lazily initialize the SparkSession, with exception handling
  lazy private val sparkSession = {
    try {
      sparkBuilder.getOrCreate()
    } catch {
      case e: Exception => {
        logger.error(s"Failed to create SparkSession", e)
        throw e
      }
    }
  }

  def getSparkSession: SparkSession = sparkSession
}
