# Data Warehouse Implementation Project

The follwing notebook has been published publicly on databricks along with the results.. [Click Here](https://databricks-prod-cloudfront.cloud.databricks.com/public/4027ec902e239c93eaaa8714f173bcfc/3969169828089009/2859625473491059/2340490761257204/latest.html).
## Extracting the Data
 1. Create tables in the hive meta store by going to `catalog` -> `create table`. Upload the csv files downloaded from [kaggle](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce?select=olist_customers_dataset.csv).
 
2. Use the UI to create table. Make sure to check `first rwow as header` and `Infer Schema` options. This will store the tables in hive metastore

## Reading to Spark DataFrame

From the hive metastore read the tables to a spark dataframe as shown below. Change the table names accordingly.

``` python
customer_dim = spark.read.table('hive_metastore.default.olist_customers_dataset_3_csv')
geolocation_dim = spark.table('hive_metastore.default.olist_geolocation_dataset_4_csv')
order_items_dim = spark.table('hive_metastore.default.olist_order_items_dataset_3_csv')
order_payments_dim = spark.table('hive_metastore.default.olist_order_payments_dataset_4_csv')
order_reviews_dim = spark.table('hive_metastore.default.olist_order_reviews_dataset_3_csv')
orders_dim = spark.table('hive_metastore.default.olist_orders_dataset_3_csv')
products_dim = spark.table('hive_metastore.default.olist_products_dataset_3_csv')
sellers_dim = spark.table('hive_metastore.default.olist_sellers_dataset_3_csv')
prod_transalation_dim = spark.table('hive_metastore.default.product_category_name_translation_7_csv')
```
## Displaying Results
Now display the tables as shown below

``` python
display(customer_dim)
display(geolocation_dim)
display(order_items_dim) 
display(order_payments_dim)
display(order_reviews_dim)
display(orders_dim)
display(products_dim)
display(sellers_dim)
display(prod_transalation_dim)
```

Display the schema as shown below
``` python
display(customer_dim.printSchema())
display(geolocation_dim.printSchema())
display(order_items_dim.printSchema()) 
display(order_payments_dim.printSchema())
display(order_reviews_dim.printSchema())
display(orders_dim.printSchema())
display(products_dim.printSchema())
display(sellers_dim.printSchema())
display(prod_transalation_dim.printSchema())
```
## Generating Date Dimension

In order to generate date dimension we need the find the max and min dates of the timestamps prsent in `orders_dim`.

``` python
from pyspark.sql import functions as F

orders_dim.select(F.max('order_purchase_timestamp')).show()
orders_dim.select(F.max('order_approved_at')).show()
orders_dim.select(F.max('order_delivered_carrier_date')).show()
orders_dim.select(F.max('order_delivered_customer_date')).show()
orders_dim.select(F.max('order_estimated_delivery_date')).show()



```

``` python
from pyspark.sql import functions as F

orders_dim.select(F.min('order_purchase_timestamp')).show()
orders_dim.select(F.min('order_approved_at')).show()
orders_dim.select(F.min('order_delivered_carrier_date')).show()
orders_dim.select(F.min('order_delivered_customer_date')).show()
orders_dim.select(F.min('order_estimated_delivery_date')).show()



```
Now use those dates to generate date dimension table and then read it into a spark dataframe.

``` python
%sql
CREATE OR REPLACE TABLE date_dim AS 
WITH days AS (
    SELECT 
        sequence(TO_DATE('2016-09-04'), TO_DATE('2018-11-12'), INTERVAL 1 DAY) AS date_range
)
SELECT 
    YEAR(date) AS year,
    QUARTER(date) AS quarter,
    MONTH(date) AS month,
    WEEKOFYEAR(date) AS week,
    DAYOFWEEK(date) AS dow,
    date
FROM (
    SELECT EXPLODE(date_range) AS date FROM days
)
ORDER BY date;
```

``` python
date_dim = spark.sql("select * from date_dim")
display(date_dim)
```
## Creating Fact table

Create a fact table by joining all the dimensions.

``` python
fact_table = (orders_dim
    .join(customer_dim, "customer_id", "inner")
    .join(order_items_dim, "order_id", "inner")
    .join(order_payments_dim, "order_id", "inner")
    .join(order_reviews_dim, "order_id", "left_outer")  
    .join(products_dim, "product_id", "inner")
    .join(sellers_dim, "seller_id", "inner")
    .join(date_dim, orders_dim["order_purchase_timestamp"].cast("date") == date_dim["date"], "inner")  
    .select(
        orders_dim.order_id,
        customer_dim.customer_id,
        products_dim.product_id,
        sellers_dim.seller_id,
        date_dim.date,
        order_items_dim.price,
        order_items_dim.freight_value,
        order_payments_dim.payment_value,
        order_reviews_dim.review_score
    )
)


display(fact_table)
```
## Analytical Queries

### Average Fulfimment Time by Product Category

``` python
from pyspark.sql.functions import col, datediff, avg

fulfillment_time_by_category = (fact_table
    .join(orders_dim, "order_id", "inner") 
    .join(products_dim, "product_id", "inner") 
    .filter(col("order_delivered_customer_date").isNotNull())  
    .withColumn("fulfillment_time", datediff(col("order_delivered_customer_date"), col("order_purchase_timestamp")))  
    .groupBy("product_category_name")
    .agg(avg("fulfillment_time").alias("avg_fulfillment_days"))  
    .orderBy(col("avg_fulfillment_days"))
)

# Converting to Pandas 
pdf7 = fulfillment_time_by_category.toPandas()


import matplotlib.pyplot as plt

# Removing nulls
pdf7 = pdf7.dropna(subset=["product_category_name"])  L


plt.figure(figsize=(12,6))
plt.bar(pdf7["product_category_name"], pdf7["avg_fulfillment_days"], color="blue")
plt.xlabel("Average Fulfillment Time (Days)")
plt.ylabel("Product Category")
plt.title("Average Fulfillment Time by Product Category")
plt.xticks(rotation=90, ha="center", va="top")  
plt.grid(axis="x", linestyle="--", alpha=0.6)
plt.tight_layout()
plt.show()

```

### Year Over Year Revenue

``` python
from pyspark.sql.functions import col, sum as _sum, lag
from pyspark.sql.window import Window
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker 

# Join fact_table with date_dim to extract the year
yearly_revenue = (fact_table
    .join(date_dim, fact_table["date"] == date_dim["date"], "inner")
    .groupBy("year")
    .agg(_sum("price").alias("total_revenue"))
    .orderBy("year")
)


window_spec = Window.orderBy("year")


yearly_revenue = (yearly_revenue
    .withColumn("prev_year_revenue", lag("total_revenue").over(window_spec))
    .withColumn("YoY_growth", 
                ((col("total_revenue") - col("prev_year_revenue")) / col("prev_year_revenue") * 100))
)


pdf_yoy = yearly_revenue.toPandas()

# year must be treated as an int
pdf_yoy["year"] = pdf_yoy["year"].astype(int)


plt.figure(figsize=(10,6))
plt.bar(pdf_yoy["year"], pdf_yoy["total_revenue"], color="blue", alpha=0.7)


plt.xlabel("Year")
plt.ylabel("Total Revenue ($)")
plt.title("Year-over-Year Revenue")
plt.xticks(pdf_yoy["year"]) 
plt.grid(axis="y", linestyle="--", alpha=0.6)


plt.gca().yaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f"{int(x):,}"))

plt.show()


```

# Average Order Value By Month To Get Seasonal Trends

``` python
from pyspark.sql.functions import col, avg, year, month
import matplotlib.pyplot as plt

# Rename the 'date' column in date_dim to avoid ambiguity
date_dim_renamed = date_dim.withColumnRenamed("date", "d_date")


avg_order_value_by_month = (fact_table
    .join(date_dim_renamed, fact_table["date"] == date_dim_renamed["d_date"], "inner")
    .groupBy(year(col("d_date")).alias("year"), month(col("d_date")).alias("month"))
    .agg(avg("price").alias("avg_order_value"))
    .orderBy("year", "month")
)


pdf_monthly_avg_order_value = avg_order_value_by_month.toPandas()

# Create a 'month_year' column for plotting
pdf_monthly_avg_order_value["month_year"] = pdf_monthly_avg_order_value["month"].astype(str) + '-' + pdf_monthly_avg_order_value["year"].astype(str)


plt.figure(figsize=(12,6))
plt.plot(pdf_monthly_avg_order_value["month_year"], pdf_monthly_avg_order_value["avg_order_value"], marker='o', color="orange")
plt.xlabel("Month-Year")
plt.ylabel("Average Order Value ($)")
plt.title("Average Order Value by Month")
plt.xticks(rotation=45)
plt.grid(True)
plt.tight_layout()
plt.show()
```

