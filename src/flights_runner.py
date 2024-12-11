from flights.transforms import flight_transforms, shared_transforms
from flights.utils import flight_utils

from flights.utils import flight_utils

from pyspark.sql.functions import col, udf
from pyspark.sql.types import StringType

catalog = "main"
database = "flights_dev"

path = "/databricks-datasets/airlines"
raw_table_name = f"{catalog}.{database}.flights_raw"

from databricks.connect import DatabricksSession
spark = DatabricksSession.builder.getOrCreate()


spark.addArtifact("src/flights/utils/flight_utils.py", pyfile=True)
# from flights.utils.flight_utils import my_split_udf

# spark.addArtifact("src/flights/utils/split_udf.py", pyfile=True) #


@udf(returnType=StringType(), useArrow=True)
def split_udf(s):
    from flight_utils import my_split
    return my_split(s)

df = flight_utils.read_batch(spark, path).limit(1000)

df_transformed = (
        df.transform(flight_transforms.delay_type_transform)
          .transform(shared_transforms.add_metadata_columns)
    )

df_transformed2 = df_transformed.withColumn("split_val", split_udf(col("UniqueCarrier")))

# df_transformed2 = df_transformed.withColumn("split_val", flights.utils.split_udf.split_udf(col("UniqueCarrier")))
# df_transformed2.show()

print(f"Reading data from {path}")
df_transformed.write.format("delta").mode("append").option("mergeSchema", "true").saveAsTable(raw_table_name)
print(f"Succesfully wrote data to {raw_table_name}")