from pyspark.sql.functions import col, udf
from pyspark.sql.types import StringType
from pyspark.sql.session import PySparkRuntimeError
import os

from flights.transforms import flight_transforms, shared_transforms
from flights.utils import flight_utils

catalog = "main"
database = "flights_dev"

path = "/databricks-datasets/airlines"
raw_table_name = f"{catalog}.{database}.flights_raw"

from databricks.connect import DatabricksSession
spark = DatabricksSession.builder.getOrCreate()


def is_running_on_databricks():
    if os.environ.get("DATABRICKS_RUNTIME_VERSION") is not None:
        return True
    else:
        return False

running_on_cluster = is_running_on_databricks()
print("Code running on Databricks (as worfklow or from workspace)?:", running_on_cluster)

if running_on_cluster != True:
    spark.addArtifact("src/flights/utils/flight_utils.py", pyfile=True)

df = flight_utils.read_batch(spark, path).limit(1000)

df_transformed = (
        df.transform(flight_transforms.delay_type_transform)
          .transform(shared_transforms.add_metadata_columns)
    )

df_transformed2 = df_transformed.withColumn("CRSDepTime", flight_utils.clean_time_udf(col("CRSDepTime")))

print(f"Reading data from {path}")
df_transformed.write.format("delta").mode("append").option("mergeSchema", "true").saveAsTable(raw_table_name)
print(f"Succesfully wrote data to {raw_table_name}")