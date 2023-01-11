import argparse
import ast

from pyspark.sql import SparkSession
from pyspark.sql.functions import col

def main(spark, source_name, cast_config, run_date):
    df = spark.read.format("xml")\
                   .options(rowTag="row", rootTag="tags")\
                   .load(f"s3://english-stackexchange-com/raw/{run_date}/{source_name}.xml")

    for column_name, type in cast_config.items():
        df = df.withColumn(column_name, col(column_name).cast(type))

    df.write.parquet(f"s3://english-stackexchange-com/parquet/{run_date}/{source_name}.parquet", mode="overwrite")

if __name__ == "__main__":
    spark = SparkSession.builder.appName("save_xml_as_parquet").getOrCreate()
    parser = argparse.ArgumentParser()
    parser.add_argument("--source-name")
    parser.add_argument("--cast-config")
    parser.add_argument("--run-date")
    args = parser.parse_args()
    source_name = args.source_name
    cast_config = ast.literal_eval(args.cast_config)
    run_date = args.run_date
    main(spark, source_name, cast_config, run_date)