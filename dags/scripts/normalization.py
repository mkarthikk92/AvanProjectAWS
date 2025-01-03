"""
Identify the entities.
1) Customer
2) Notification Preferences
3) Orders
4) Items

Identify the grain.
We need the lowest level of grain. Let's say we want the transactions happened per day.

Identify the Fact table.
The fact table will be the Orders and Items table.

Identify the Dimensions.
The dimensions will be the Customer and Orders.

Create dataframes for each of these.
"""

df_customer_schema = StructType(
    [
        StructField('customer_id', IntegerType(), False),
        StructField('name', StringType(), False),
        StructField('email', StringType(), False),
        StructField('age', IntegerType(), True),
    ]
)

df_notifications_schema = StructType(
    [
        StructField('customer_id', IntegerType(), False),
        StructField('email_preference', BooleanType(), True),
        StructField('sms_preference', BooleanType(), True)
    ]
)

df_order_schema = StructType(
    [
        StructField('customer_id', IntegerType(), False),
        StructField('order_id', StringType(), False),
        StructField('amount', FloatType(), False),
        StructField('order_date', DateType(), False)
    ]
)

df_item_schema = StructType(
    [
        StructField('order_id', StringType(), False),
        StructField('item_id', StringType(), False),
        StructField('item_name', StringType(), False),
        StructField('item_quantity', IntegerType(), True),
        StructField('item_price', FloatType(), True),
    ]
)

df_customer = (
    df_explode_items
    .select(
        col('id').cast(IntegerType()).alias('customer_id'),
        col('name').cast(StringType()).alias('name'),
        col('email').cast(StringType()).alias('email'),
        col('age').cast(IntegerType()).alias('age')
    ).distinct()
)

# df_customer.show(5, truncate=False)

df_preference = (
    df_explode_items
    .select(
        col('id').cast(IntegerType()).alias('customer_id'),
        col('email_preference').cast(BooleanType()),
        col('sms_preference').cast(BooleanType())
    ).distinct()
)

# df_preference.show(5, truncate=False)

df_order = (
    df_explode_items
    .select(
        col('id').cast(StringType()).alias('customer_id'),
        col('order_id').cast(StringType()),
        col('amount').cast(FloatType()),
        col('order_date').cast(DateType())
    ).distinct()
)

# df_order.show(5, False)

df_item = (
    df_explode_items
    .select(
        col('order_id').cast(StringType()),
        col('item_id').cast(StringType()),
        col('item_name').cast(StringType()),
        col('quantity').cast(IntegerType()),
        col('price').cast((FloatType()))
    ).distinct()
)

# items_count = df_item.groupby(col('item_name')).count()
# items_count.show()

"""
Business Questions:
1) Total amount for all transactions per day?
2) Find the order_id and customer_id which has the most (quantity * price) for any item?
"""

total_amount = (
    df_order
    .groupby(col('order_date'))
    .agg(sum('amount'))
    .orderBy('order_date')
)

total_amount.show()

window_spec = Window.orderBy(col('product').desc())

product_item = (
    df_order.alias('lft').join(df_item.alias('rht'), on=['order_id'], how='inner')
    .withColumn('product', col('quantity') * col('price'))
    .select(col('order_id'), col('lft.customer_id'), col('item_id'), col('product'), rank().over(window_spec).alias('rnk'))
)

highest_item_order = (
    product_item
    .filter(col('rnk') == 1)
    .select(
        col('order_id'),
        col('customer_id'),
        col('item_id'),
        col('product')
    )
)

highest_item_order_customer = (
    highest_item_order
    .join(df_customer, on=['customer_id'], how='inner')
    .select(col('name'), col('order_id'), col('item_id'), col('product'))
)

highest_item_order_customer.show(truncate=False)

