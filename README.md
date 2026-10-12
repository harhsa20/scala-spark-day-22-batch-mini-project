# Day 22 — E-Commerce Batch Sales Pipeline

## Objective
Build an end-to-end batch data pipeline using Scala and Apache Spark.

## Technologies
- Scala 2.12.18
- Apache Spark 3.5.3
- sbt 1.12.15
- Java 17

## Pipeline Workflow
1. Read transaction, customer, and product CSV files.
2. Convert quantity and unit price into numeric values.
3. Reject invalid transactions.
4. Join transactions with customer and product details.
5. Calculate transaction revenue.
6. Aggregate daily sales by date, city, and category.
7. Write partitioned Parquet output.
8. Read the Parquet output to validate the results.

## Data Quality Results
- Raw transactions: 12
- Valid transactions: 8
- Rejected transactions: 4

## Daily Revenue
| Date | Revenue |
|---|---:|
| 2026-10-10 | 2950.0 |
| 2026-10-11 | 3900.0 |
| 2026-10-12 | 2100.0 |
| Total | 8950.0 |

## Output
Partitioned Parquet data is written to `output/daily_sales`, partitioned by year, month, and day.

## Run
```bash
sbt "runMain BatchSalesPipeline"
