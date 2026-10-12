"""End-to-end check: Spark -> Nessie -> MinIO.
Run: docker exec spark-iceberg spark-submit /home/iceberg/scripts/00_smoke_test.py
"""
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("smoke-test").getOrCreate()
spark.sql("CREATE NAMESPACE IF NOT EXISTS nessie.lab")
spark.sql("DROP TABLE IF EXISTS nessie.lab.smoke")
spark.sql("CREATE TABLE nessie.lab.smoke (id INT, city STRING) USING iceberg")
spark.sql("INSERT INTO nessie.lab.smoke VALUES (1,'Riyadh'),(2,'Cairo'),(3,'Khobar')")
n = spark.sql("SELECT count(*) c FROM nessie.lab.smoke").first().c
assert n == 3, f"expected 3 rows, got {n}"
spark.sql("SELECT snapshot_id, operation FROM nessie.lab.smoke.snapshots").show(truncate=False)
print("SMOKE TEST PASSED - now query iceberg.lab.smoke from Trino")
