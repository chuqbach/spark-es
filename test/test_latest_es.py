from pyspark.sql import SparkSession, DataFrame
import pyspark.sql.functions as F


spark_config = {
    "spark.app.name": "Elastic Spark",
    "spark.jars.packages": "org.elasticsearch:elasticsearch-hadoop:8.3.1",
    # "spark.jars": "jars/elasticsearch-spark-20_2.11-8.3.1.jar",
    "spark.es.nodes": "myIP",
    "spark.es.port": "9200",
    "spark.es.index.auto.create": "true",
    "spark.es.nodes.wan.only": "true",
}


def main():
    spark_builder = SparkSession.builder
    for key, value in spark_config.items():
        spark_builder.config(key, value)

    spark = spark_builder.getOrCreate()
    index = "spark-index"
    df: DataFrame = spark.range(10).toDF("number")
    df = df.withColumn("doubled number", F.expr("number * 2"))
    df.show()

    (
        df.write.format("org.elasticsearch.spark.sql")
            .mode("append")
            .save(index)
    )


if __name__ == "__main__":
    main()
