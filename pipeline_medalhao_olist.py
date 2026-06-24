# Databricks notebook source
# MAGIC %md
# MAGIC # Data Lake de E-Commerce — Arquitetura Medalhão (Olist)
# MAGIC Pipeline Bronze → Silver → Gold em Delta Lake sobre a base pública Olist.
# MAGIC
# MAGIC **Pré-requisitos:**
# MAGIC 1. `CREATE SCHEMA IF NOT EXISTS workspace.olist;`
# MAGIC 2. `CREATE VOLUME IF NOT EXISTS workspace.olist.raw;`
# MAGIC 3. Subir os 9 CSVs do Olist em `/Volumes/workspace/olist/raw/`
# MAGIC
# MAGIC Rode as células de cima para baixo.

# COMMAND ----------

# MAGIC %md ## Configuração

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql import Window

CATALOG = "workspace"
SCHEMA  = "olist"
RAW     = f"/Volumes/{CATALOG}/{SCHEMA}/raw"

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")
spark.sql(f"USE {CATALOG}.{SCHEMA}")

# nome do arquivo -> nome lógico da tabela bronze
SOURCES = {
    "olist_orders_dataset.csv":               "bronze_orders",
    "olist_order_items_dataset.csv":          "bronze_order_items",
    "olist_order_payments_dataset.csv":       "bronze_payments",
    "olist_order_reviews_dataset.csv":        "bronze_reviews",
    "olist_customers_dataset.csv":            "bronze_customers",
    "olist_products_dataset.csv":             "bronze_products",
    "olist_sellers_dataset.csv":              "bronze_sellers",
    "olist_geolocation_dataset.csv":          "bronze_geolocation",
    "product_category_name_translation.csv":  "bronze_category_translation",
}

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. BRONZE — ingestão fiel dos dados crus
# MAGIC Lê cada CSV, adiciona metadados de ingestão (`_ingest_timestamp`, `_source_file`) e grava como tabela Delta gerenciada. Nenhuma transformação de negócio aqui.

# COMMAND ----------

for filename, table in SOURCES.items():
    df = (
        spark.read
        .option("header", True)
        .option("inferSchema", True)
        .option("multiLine", True)        # reviews têm texto com quebras de linha
        .option("escape", '"')
        .csv(f"{RAW}/{filename}")
        .withColumn("_ingest_timestamp", F.current_timestamp())
        .withColumn("_source_file", F.lit(filename))
    )
    df.write.mode("overwrite").option("overwriteSchema", True).saveAsTable(table)
    print(f"[bronze] {table:32s} linhas = {df.count():>8d}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. SILVER — limpo, tipado, deduplicado
# MAGIC Regras de qualidade, conversão de tipos, tradução de categorias e cálculo de métricas de entrega.

# COMMAND ----------

# --- silver_orders: tipa datas, calcula prazo e pontualidade -----------------
orders = spark.table("bronze_orders")
silver_orders = (
    orders
    .withColumn("order_purchase_timestamp",       F.to_timestamp("order_purchase_timestamp"))
    .withColumn("order_delivered_customer_date",   F.to_timestamp("order_delivered_customer_date"))
    .withColumn("order_estimated_delivery_date",   F.to_timestamp("order_estimated_delivery_date"))
    .withColumn("purchase_date", F.to_date("order_purchase_timestamp"))
    .withColumn(
        "delivery_days",
        F.datediff("order_delivered_customer_date", "order_purchase_timestamp"),
    )
    .withColumn(
        "on_time_flag",
        F.when(
            F.col("order_delivered_customer_date") <= F.col("order_estimated_delivery_date"), 1
        ).when(F.col("order_delivered_customer_date").isNotNull(), 0)
         .otherwise(None),
    )
    .dropDuplicates(["order_id"])
)
silver_orders.write.mode("overwrite").option("overwriteSchema", True).saveAsTable("silver_orders")
print("silver_orders:", silver_orders.count())

# COMMAND ----------

# --- silver_order_items: tipa preço e frete ----------------------------------
silver_order_items = (
    spark.table("bronze_order_items")
    .withColumn("price",         F.col("price").cast("double"))
    .withColumn("freight_value", F.col("freight_value").cast("double"))
)
silver_order_items.write.mode("overwrite").option("overwriteSchema", True).saveAsTable("silver_order_items")
print("silver_order_items:", silver_order_items.count())

# COMMAND ----------

# --- silver_products: traduz categoria para inglês ---------------------------
products    = spark.table("bronze_products")
translation = spark.table("bronze_category_translation")
silver_products = (
    products.join(translation, on="product_category_name", how="left")
    .withColumn(
        "category_english",
        F.coalesce(F.col("product_category_name_english"), F.lit("unknown")),
    )
    .select(
        "product_id",
        "product_category_name",
        "category_english",
        F.col("product_weight_g").cast("double").alias("product_weight_g"),
    )
    .dropDuplicates(["product_id"])
)
silver_products.write.mode("overwrite").option("overwriteSchema", True).saveAsTable("silver_products")
print("silver_products:", silver_products.count())

# COMMAND ----------

# --- silver_customers / silver_sellers: geografia ----------------------------
(
    spark.table("bronze_customers")
    .select("customer_id", "customer_unique_id", "customer_city", "customer_state")
    .dropDuplicates(["customer_id"])
    .write.mode("overwrite").option("overwriteSchema", True).saveAsTable("silver_customers")
)
(
    spark.table("bronze_sellers")
    .select("seller_id", "seller_city", "seller_state")
    .dropDuplicates(["seller_id"])
    .write.mode("overwrite").option("overwriteSchema", True).saveAsTable("silver_sellers")
)
print("silver_customers / silver_sellers OK")

# COMMAND ----------

# --- silver_payments: agrega por pedido (grão = order_id) --------------------
silver_payments = (
    spark.table("bronze_payments")
    .withColumn("payment_value", F.col("payment_value").cast("double"))
    .groupBy("order_id")
    .agg(
        F.sum("payment_value").alias("payment_value"),
        F.countDistinct("payment_type").alias("payment_type_count"),
    )
)
silver_payments.write.mode("overwrite").option("overwriteSchema", True).saveAsTable("silver_payments")
print("silver_payments:", silver_payments.count())

# COMMAND ----------

# --- silver_reviews: uma nota por pedido -------------------------------------
w = Window.partitionBy("order_id").orderBy(F.col("review_creation_date").desc())
silver_reviews = (
    spark.table("bronze_reviews")
    .withColumn("review_score", F.col("review_score").cast("int"))
    .withColumn("_rn", F.row_number().over(w))
    .filter(F.col("_rn") == 1)
    .select("order_id", "review_score")
)
silver_reviews.write.mode("overwrite").option("overwriteSchema", True).saveAsTable("silver_reviews")
print("silver_reviews:", silver_reviews.count())

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. GOLD — modelo dimensional + data marts de negócio

# COMMAND ----------

# --- gold_dim_date -----------------------------------------------------------
bounds = silver_orders.select(
    F.min("purchase_date").alias("dmin"), F.max("purchase_date").alias("dmax")
).first()

gold_dim_date = (
    spark.sql(f"SELECT explode(sequence(to_date('{bounds.dmin}'), to_date('{bounds.dmax}'), interval 1 day)) AS Date")
    .withColumn("Year",       F.year("Date"))
    .withColumn("MonthNo",    F.month("Date"))
    .withColumn("Month",      F.date_format("Date", "MMM"))
    .withColumn("YearMonth",  F.date_format("Date", "yyyy-MM"))
    .withColumn("Quarter",    F.concat(F.lit("Q"), F.quarter("Date")))
)
gold_dim_date.write.mode("overwrite").option("overwriteSchema", True).saveAsTable("gold_dim_date")

# --- dimensões ---------------------------------------------------------------
spark.table("silver_products").write.mode("overwrite").option("overwriteSchema", True).saveAsTable("gold_dim_product")
spark.table("silver_customers").write.mode("overwrite").option("overwriteSchema", True).saveAsTable("gold_dim_customer")
spark.table("silver_sellers").write.mode("overwrite").option("overwriteSchema", True).saveAsTable("gold_dim_seller")
print("dimensões OK")

# COMMAND ----------

# --- gold_fact_order_items (grão = item de pedido) ---------------------------
oi     = spark.table("silver_order_items")
orders = spark.table("silver_orders").select("order_id", "purchase_date", "customer_id")
prod   = spark.table("silver_products").select("product_id", "category_english")
cust   = spark.table("silver_customers").select("customer_id", "customer_state")
sell   = spark.table("silver_sellers").select("seller_id", "seller_state")

gold_fact_order_items = (
    oi.join(orders, "order_id", "left")
      .join(prod, "product_id", "left")
      .join(cust, "customer_id", "left")
      .join(sell, "seller_id", "left")
      .select(
          "order_id", "order_item_id", "purchase_date",
          "product_id", "category_english",
          "seller_id", "seller_state",
          "customer_id", "customer_state",
          "price", "freight_value",
      )
)
gold_fact_order_items.write.mode("overwrite").option("overwriteSchema", True).saveAsTable("gold_fact_order_items")
print("gold_fact_order_items:", gold_fact_order_items.count())

# COMMAND ----------

# --- gold_orders_enriched (grão = pedido) ------------------------------------
items_agg = (
    spark.table("silver_order_items")
    .groupBy("order_id")
    .agg(
        F.sum("price").alias("order_revenue"),
        F.sum("freight_value").alias("order_freight"),
        F.count("*").alias("item_count"),
    )
)
gold_orders_enriched = (
    spark.table("silver_orders")
    .select("order_id", "purchase_date", "customer_id",
            "order_status", "delivery_days", "on_time_flag")
    .join(items_agg, "order_id", "left")
    .join(spark.table("silver_payments"), "order_id", "left")
    .join(spark.table("silver_reviews"),  "order_id", "left")
    .join(spark.table("silver_customers").select("customer_id", "customer_state"),
          "customer_id", "left")
)
gold_orders_enriched.write.mode("overwrite").option("overwriteSchema", True).saveAsTable("gold_orders_enriched")
print("gold_orders_enriched:", gold_orders_enriched.count())

# COMMAND ----------

# --- marts de negócio --------------------------------------------------------
# Receita por categoria e mês
(
    gold_fact_order_items
    .withColumn("year_month", F.date_format("purchase_date", "yyyy-MM"))
    .groupBy("year_month", "category_english")
    .agg(
        F.sum("price").alias("revenue"),
        F.countDistinct("order_id").alias("orders"),
        F.count("*").alias("items"),
    )
    .write.mode("overwrite").option("overwriteSchema", True)
    .saveAsTable("gold_revenue_by_category_month")
)

# Performance de vendedores
(
    gold_fact_order_items
    .groupBy("seller_id", "seller_state")
    .agg(
        F.sum("price").alias("revenue"),
        F.countDistinct("order_id").alias("orders"),
        F.avg("price").alias("avg_item_price"),
    )
    .orderBy(F.col("revenue").desc())
    .write.mode("overwrite").option("overwriteSchema", True)
    .saveAsTable("gold_seller_performance")
)
print("marts OK")

# COMMAND ----------

# MAGIC %md ## 4. Verificação rápida

# COMMAND ----------

display(spark.sql("SHOW TABLES"))

# COMMAND ----------

display(spark.sql("""
    SELECT category_english,
           ROUND(SUM(price), 2)        AS revenue,
           COUNT(DISTINCT order_id)    AS orders
    FROM gold_fact_order_items
    GROUP BY category_english
    ORDER BY revenue DESC
    LIMIT 15
"""))

# COMMAND ----------

display(spark.sql("""
    SELECT ROUND(AVG(delivery_days), 1)                  AS avg_delivery_days,
           ROUND(AVG(on_time_flag) * 100, 1)             AS on_time_pct,
           ROUND(AVG(review_score), 2)                   AS avg_review
    FROM gold_orders_enriched
    WHERE delivery_days IS NOT NULL
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ### (Opcional) Exportar gold para o Power BI consumir sem credencial
# MAGIC Descomente para gravar CSVs no Volume — depois baixe e use no Power BI Desktop.

# COMMAND ----------

# for t in ["gold_fact_order_items", "gold_orders_enriched", "gold_dim_product",
#           "gold_dim_customer", "gold_dim_seller", "gold_dim_date",
#           "gold_revenue_by_category_month", "gold_seller_performance"]:
#     (spark.table(t).toPandas()
#         .to_csv(f"/Volumes/{CATALOG}/{SCHEMA}/raw/export_{t}.csv", index=False))
#     print("exportado:", t)
