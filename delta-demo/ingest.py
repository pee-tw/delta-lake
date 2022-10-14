import dload

import shutil
from pyspark.sql.functions import year

from delta import *
from pyspark.sql import SparkSession

delta_spark_version = "2.1.0"
spark_jars_packages = f"io.delta:delta-core_2.12:{delta_spark_version}"

spark = (
    SparkSession.builder.master("local[*]")
    .appName("PySparkLocal")
    .config("spark.executor.memory", "4g")
    .config("spark.driver.memory", "2g")
    .config("spark.memory.offHeap.enabled", True)
    .config("spark.memory.offHeap.size", "8g")
    .config("spark.jars.packages", spark_jars_packages)
    .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config(
        "spark.sql.catalog.spark_catalog",
        "org.apache.spark.sql.delta.catalog.DeltaCatalog",
    )
    .config("spark.databricks.delta.schema.autoMerge.enabled", "true")
    .config("spark.databricks.delta.retentionDurationCheck.enabled", "false")
    .config("spark.databricks.delta.optimize.repartition.enabled", "true")
    .getOrCreate()
)

from pyspark.sql.types import (
    StructType,
    IntegerType,
    LongType,
    DoubleType,
    BooleanType,
)
from pyspark.sql.functions import lit

schema = (
    StructType()
    .add("tradeId", IntegerType(), True)
    .add("price", DoubleType(), True)
    .add("qty", DoubleType(), True)
    .add("quoteQty", DoubleType(), True)
    .add("time", LongType(), True)
    .add("isBuyerMaker", BooleanType(), True)
    .add("isBestMatch", BooleanType(), True)
)

tradingPair = "BTCUSDT"

from datetime import date, timedelta


def daterange(start_date, end_date):
    for n in range(int((end_date - start_date).days)):
        yield start_date + timedelta(n)

# start_date = date(2021, 2, 28)
start_date = date(2021, 3, 14)
end_date = date(2022, 10, 10)

for single_date in daterange(start_date, end_date):
    isoDate = single_date.strftime("%Y-%m-%d")
    year = isoDate.split("-")[0]
    downloadString = f"{tradingPair}-trades-{isoDate}"
    downloadedPath = dload.save_unzip(
        f"https://data.binance.vision/data/spot/daily/trades/{tradingPair}/{downloadString}.zip",
        delete_after=True,
    )
    
    df = spark.read.schema(schema).csv(f"{downloadedPath}/{downloadString}.csv")
    df = (
        df.withColumn("tradingPair", lit(tradingPair))
        .withColumn("year", lit(year))
        .withColumn("isoDate", lit(isoDate))
    )

    df.write.format("delta").partitionBy(["year", "tradingPair"]).mode(
        "overwrite"
    ).option(
        "replaceWhere", f"isoDate == '{isoDate}' AND tradingPair == '{tradingPair}'"
    ).save(
        "Deltalake/crypto"
    )

    shutil.rmtree(downloadedPath)
    print(f"Ingested: {downloadString}")