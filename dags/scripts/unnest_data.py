import sys
from pyspark.context import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions

# Fetching job arguments
args = getResolvedOptions(sys.argv, ['JOB_NAME', 'INPUT_PATH', 'OUTPUT_PATH'])

# Initialize Glue context
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.sp

job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Read JSON input data
input_path = args['INPUT_PATH']
df = (
    spark
    .read
    .format('json')
    .option('multiLine', True)
    .option('inferSchema', False)
    .load(input_path)
)

# Explode and transform the data
df_explode_orders = (
    df
    .withColumn('map_keys', col('preferences.notifications'))
    .withColumn('orders_explode', explode(col('orders')))
)

df_explode_items = (
    df_explode_orders
    .withColumn('orders_items', explode(col('orders_explode.items')))
    .select(
        col('id'),
        col('name'),
        col('email'),
        col('age'),
        col('registered_at'),
        col('map_keys.email').alias('email_preference'),
        col('map_keys.sms').alias('sms_preference'),
        col('orders_explode.order_id'),
        col('orders_explode.amount'),
        col('orders_explode.order_date'),
        col('orders_items.item_id'),
        col('orders_items.name').alias('item_name'),
        col('orders_items.quantity'),
        col('orders_items.price')
    )
)

# Write the transformed data to the specified output path
output_path = args['OUTPUT_PATH']
(
    df_explode_items
    .write
    .mode('overwrite')
    .format('csv')  # You can change this to 'json', 'csv', etc.
    .save(output_path)
)

# Commit the Glue job
job.commit()
