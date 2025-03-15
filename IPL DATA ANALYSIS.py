# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, avg, max, min, count, desc


spark = SparkSession.builder.appName("IPL Data Analysis").getOrCreate()


batsman_df = spark.read.csv("dbfs:/FileStore/tables/IPL2023_Batsman.csv", header=True, inferSchema=True)
bowler_df = spark.read.csv("dbfs:/FileStore/tables/IPL2023_Bowler.csv", header=True, inferSchema=True)
scoreboard_df = spark.read.csv("dbfs:/FileStore/tables/IPL2023_Match_Scoreboard.csv", header=True, inferSchema=True)
matches_df = spark.read.csv("dbfs:/FileStore/tables/IPL2023_Matches.csv", header=True, inferSchema=True)

#
batsman_df.printSchema()
batsman_df.show(5)

bowler_df.printSchema()
bowler_df.show(5)

scoreboard_df.printSchema()
scoreboard_df.show(5)

matches_df.printSchema()
matches_df.show(5)


# COMMAND ----------

print(f"Batsman records: {batsman_df.count()}")
print(f"Bowler records: {bowler_df.count()}")
print(f"Scoreboard records: {scoreboard_df.count()}")
print(f"Matches records: {matches_df.count()}")


# COMMAND ----------

from pyspark.sql.functions import sum, desc

top_batsmen = (
    batsman_df
    .groupBy("Batsman") 
    .agg(sum("Run").alias("Total_Runs"))  
    .orderBy(desc("Total_Runs"))
)

top_batsmen.show(10)





# COMMAND ----------

from pyspark.sql.functions import sum, desc

top_bowlers = (
    bowler_df
    .groupBy("Bowler")  
    .agg(sum("wicket").alias("Total_Wickets"))  
    .orderBy(desc("Total_Wickets"))
)

top_bowlers.show(10)



# COMMAND ----------

from pyspark.sql.functions import avg, sum


total_runs_per_match = (
    scoreboard_df
    .groupBy("match_no")
    .agg((sum("Home_team_run") + sum("Away_team_run")).alias("Total_Runs"))
)


avg_score_per_match = total_runs_per_match.agg(avg("Total_Runs").alias("Avg_Total_Runs"))


avg_score_per_match.show()



# COMMAND ----------

from pyspark.sql.functions import col, desc


scoreboard_df = scoreboard_df.withColumn("Total_Runs", col("Home_team_run") + col("Away_team_run"))


highest_home_score = scoreboard_df.select("match_no", "Home_team_run", "Total_Runs").orderBy(desc("Home_team_run")).limit(1)
highest_away_score = scoreboard_df.select("match_no", "Away_team_run", "Total_Runs").orderBy(desc("Away_team_run")).limit(1)


highest_score = highest_home_score.union(highest_away_score)


lowest_home_score = scoreboard_df.select("match_no", "Home_team_run", "Total_Runs").orderBy("Home_team_run").limit(1)
lowest_away_score = scoreboard_df.select("match_no", "Away_team_run", "Total_Runs").orderBy("Away_team_run").limit(1)


lowest_score = lowest_home_score.union(lowest_away_score)


print("Highest Score:")
highest_score.show()

print("Lowest Score:")
lowest_score.show()


# COMMAND ----------

display(dbutils.fs.ls("/FileStore/tables/"))


# COMMAND ----------

from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("IPL Data Analysis").getOrCreate()


batsman_df = spark.read.csv("dbfs:/FileStore/tables/IPL2023_Batsman.csv", header=True, inferSchema=True)


batsman_df.show(5)


# COMMAND ----------

batsman_df.printSchema()


# COMMAND ----------

from pyspark.sql.functions import col, sum, desc
import matplotlib.pyplot as plt
import pandas as pd


top_batsmen = (
    batsman_df.groupBy("Batsman")  
    .agg(sum("Run").alias("Total_Runs"))  
    .orderBy(desc("Total_Runs"))
)


top_batsmen_pd = top_batsmen.limit(10).toPandas()


plt.figure(figsize=(10, 6))
plt.bar(top_batsmen_pd["Batsman"], top_batsmen_pd["Total_Runs"], color="blue")
plt.xticks(rotation=45)
plt.xlabel("Players")
plt.ylabel("Runs")
plt.title("Top 10 Run Scorers")
plt.show()


# COMMAND ----------


from pyspark.sql import SparkSession
from pyspark.sql.functions import count, desc
import matplotlib.pyplot as plt


spark = SparkSession.builder.appName("IPL Data Analysis").getOrCreate()


matches_df = spark.read.csv("dbfs:/FileStore/tables/IPL2023_Matches.csv", header=True, inferSchema=True)


matches_df.printSchema()


matches_won = (
    matches_df.groupBy("Winner")  
    .agg(count("*").alias("count"))
    .orderBy(desc("count"))
)


matches_won_pd = matches_won.toPandas()


plt.figure(figsize=(8, 8))
plt.pie(
    matches_won_pd["count"], 
    labels=matches_won_pd["Winner"], 
    autopct='%1.1f%%', 
    startangle=140
)
plt.title("Matches Won by Each Team")
plt.axis('equal')  
plt.show()

