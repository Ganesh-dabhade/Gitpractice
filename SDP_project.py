from pyspark.sql import SparkSession
from pyspark.sql.functions import col,concat,length,min,max
spark=SparkSession. \
    builder. \
    master("local[2]"). \
    appName("SDP_project"). \
    getOrCreate()

Schema="Actor_name string, movie_name1 string, year string"
Movie_df= spark.read.csv("C:/Users/Ganesh Dabhade/PycharmProjects/pythonProject3/data_files/movies - Copy.tsv",
                         schema=Schema, sep="\t")
#Movie_df.show()
r_schema="Rating float, movie_name2 string, year1 string"
Movie_rate_df= spark.read.csv("C:/Users/Ganesh Dabhade/PycharmProjects/pythonProject3/data_files/movie-ratings.tsv",
                         schema=r_schema, sep="\t")
Movie_rate_df.show()

print("Answer_1")
q1_df=Movie_df.groupby("year").count().orderBy(col("count").desc()).show()

print("Answer-2")
q2_df= Movie_df.groupby("Actor_name").count().orderBy(col("count").desc()).show()

print("Answer_3")
high_rate_df=Movie_rate_df.groupby("year1").max("Rating")
max_df=high_rate_df.select(high_rate_df["year1"].alias("year"),high_rate_df["max(Rating)"].alias("maximum"))
cond=[max_df.maximum == Movie_rate_df.Rating,max_df.year==Movie_rate_df.year1]
df2=max_df.join(Movie_rate_df,cond, how="inner")

df3=df2.join(Movie_df, on=df2.movie_name2== Movie_df.movie_name1, how="inner"). \
select("year1","Rating","movie_name1","Actor_name").orderBy("year1")
df3.show()

print("Answer_4")
q4d1=Movie_df.select(Movie_df["Actor_name"].alias("Actor"))
# q4d1.show()
# Movie_df.show()
# ans4=q4d1.crossJoin(Movie_df).filter("Actor != Actor_name").show()
mo_df=Movie_df.alias("movie1").join(Movie_df.alias("movie2"),col("movie1.movie_name1") == col("movie2.movie_name1"),"inner"). \
groupBy("movie1.Actor_name").count().orderBy(col("count").desc())
mo_df.show()

print("Answer_5")
df5=Movie_rate_df.select(min(col("Rating")).alias("minimum"))
df6=df5.join(Movie_rate_df, on=Movie_rate_df.Rating==df5.minimum, how="inner")
df6.join(Movie_df, on=df6.movie_name2==Movie_df.movie_name1).select("Rating", 'movie_name2','Actor_name','year'). \
    orderBy(col("movie_name2")).show()
print("Answer_6")
m_df=Movie_df.join(Movie_rate_df, on=Movie_df.movie_name1== Movie_rate_df.movie_name2, how="inner"). \
    filter("Rating BETWEEN 3 AND 8 And year1%4==0 ")
m_df.groupby("movie_name1").count().filter(col("count")>2).show()

print("Answer_9")

Movie_rate_df.withColumn("len",length("movie_name2")).filter("Rating>len").show()

print("Answer_10")
Movie_rate_df.withColumn("len",length("movie_name2")).orderBy(col("movie_name2"),col("len"),col("Rating")).show()
