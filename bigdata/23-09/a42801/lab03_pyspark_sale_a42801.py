
from pyspark.sql import SparkSession
from pyspark.sql.functions import col,when, sum as _sum, round as _round

# khoi tao sparksession

spark = SparkSession.builder \
    .appName("Lap03_Pyspark_Sales") \
        .getOrCreate()
        
print("SparkSession da khoi tao thanh cong")
print(spark)

# doc file du lieu va kiem tra
file_path = "sample_sales_bigdata_lab03.csv"

df = spark.read.csv(file_path, header=True, inferSchema=True)

print("\n--- SCHEMA cua du lieu ---")
df.printSchema()

print(f"\n--- tong so dong ban dau: {df.count()} ---")

print(f"\n--- 5 dong dau tien (df.show(5))---")

df.show(5)

# loc du lieu trang thai kh hop le
df_clean = df.dropna(subset=["city", "category", "quantity", "unit_price"]) \
             .filter((col("quantity") > 0) & (col("unit_price") > 0))

if "status" in df_clean.columns:
    df_clean = df_clean.filter(col("status").isin(["COMPLETED", "RETURNED"]))

print(f"\n--- so dong sau khi lam sach: {df_clean.count()} ---")


# tao cot revenue


if "discount" in df_clean.columns:
    df_clean = df_clean.withColumn(
        "discount", 
        when(col("discount").isNull(), 0.0).otherwise(col("discount"))
    )
    df_clean = df_clean.withColumn("revenue", _round(col("quantity") * col("unit_price") * (1 - col("discount")), 2))
else:
    df_clean = df_clean.withColumn("revenue", _round(col("quantity") * col("unit_price"), 2))

print("\n--- du lieu sau khi them cot revenue ---")
df_clean.select("city", "category", "quantity", "unit_price", "revenue").show(5)

# tong hop doanh thu theo tp

sales_by_city = df_clean.groupBy("city") \
    .agg(_round(_sum("revenue"), 2).alias("total_revenue")) \
    .orderBy(col("total_revenue").desc())

print("\n--- bang doanh thu theo thanh pho ---")
sales_by_city.show()

# doanh thu theo san pham


sales_by_category = df_clean.groupBy("category") \
    .agg(_round(_sum("revenue"), 2).alias("total_revenue")) \
    .orderBy(col("total_revenue").desc())

print("\n--- bang doanh thu theo danh muc san pham ---")
sales_by_category.show()

#xuat file du lieu

import os
import csv

os.makedirs("output", exist_ok=True)


city_rows = sales_by_city.collect()
with open("output/sales_by_city.csv", mode="w", newline="", encoding="utf-8") as f:
    writer = csv.writer(f)
    writer.writerow(sales_by_city.columns)
    for r in city_rows:
        writer.writerow(list(r))


category_rows = sales_by_category.collect()
with open("output/sales_by_category.csv", mode="w", newline="", encoding="utf-8") as f:
    writer = csv.writer(f)
    writer.writerow(sales_by_category.columns)
    for r in category_rows:
        writer.writerow(list(r))

print("\n--- xuat thanh cong file du lieu 'output' ---")

spark.stop()