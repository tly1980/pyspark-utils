import argparse
import logging


from pyspark.context import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import explode


AP = argparse.ArgumentParser()
AP.add_argument('src_db')
AP.add_argument('dst_db')
AP.add_argument('--partitionOverwriteMode', default='dynamic', type=str)

logging.basicConfig(level=logging.INFO, format='%(asctime)-15s %(message)s')


def check_in_table(spark, src, dest):
  src_df = spark.table(src)
  count = src_df.count()
  if count > 0:
    logging.info(
        '{src} [{count} rows] will be check in to {dest}'.format(
            src=src, dest=dest, count=count)
    )
    src_df.write.insertInto(dest, overwrite=True)
  else:
    logging.warn('{src} has 0 rows, skipping check-in'.format(src=src))


def check_in_database(spark, src_db, dest_db):
  for t in spark.catalog.listTables(src_db):
    src_table = '{}.{}'.format(src_db, t.name)
    dest_table = '{}.{}'.format(dest_db, t.name)
    check_in_table(src_table, dest_table)


def main(args):
  SparkContext._ensure_initialized()

  try:
    # Try to access HiveConf, it will raise exception if Hive is not added
    SparkContext._jvm.org.apache.hadoop.hive.conf.HiveConf()
    spark = SparkSession.builder\
        .enableHiveSupport()\
        .getOrCreate()
  except py4j.protocol.Py4JError:
    spark = SparkSession.builder.getOrCreate()
  except TypeError:
    spark = SparkSession.builder.getOrCreate()

  try:
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", args.partitionOverwriteMode)
    check_in_database(spark, src_db, dest_db)
  finally:
    if spark:
      spark.sparkContext.stop()


if __name__ == '__main__':
  main(AP.parse_args())
