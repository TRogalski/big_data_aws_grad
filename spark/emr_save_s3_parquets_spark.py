import argparse
import json

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, IntegerType, BooleanType


def main(spark, source_name, run_date, file_config):

    schema = StructType.fromJson(file_config["schema"])
    dest_file_name = file_config["file_name"]
    column_renames = file_config["column_renames"]

    # read xml using schema
    df = spark.read.schema(schema)\
                   .format("xml")\
                   .options(rowTag="row", rootTag="tags")\
                   .load(f"s3://english-stackexchange-com/raw/{run_date}/{source_name}.xml")

    # change column names
    df = df.toDF(*column_renames)

    # write parquet version of the file with changed name under date of the run
    df.write.parquet(f"s3://english-stackexchange-com/parquet/{run_date}/{dest_file_name}.parquet", mode="overwrite")

if __name__ == "__main__":
    spark = SparkSession.builder.appName("save_xml_as_parquet").getOrCreate()
    parser = argparse.ArgumentParser()
    parser.add_argument("--source-name")
    parser.add_argument("--file-config")
    parser.add_argument("--run-date")
    args = parser.parse_args()
    source_name = args.source_name
    file_config = json.loads(str(args.file_config))
    run_date = args.run_date
    main(spark, source_name, run_date, file_config)