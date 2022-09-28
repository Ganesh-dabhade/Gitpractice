from pyspark.sql import SparkSession
from pyspark.sql.functions import col,concat, min,max,length
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
q2_df= Movie_df.groupby("movie_count").count().orderBy(col("count").desc()).show()

print("Answer_3")

high_rate_df=Movie_rate_df.groupby("year1").max("Rating").show()


Movie_df.join(Movie_rate_df, on=Movie_df.movie_name1== Movie_rate_df.movie_name2, how="inner").groupby(col("year")).max("Rating").show()

print("Answer_4")


print("Answer_5")
Movie_df.join(Movie_rate_df, on=Movie_df.movie_name1== Movie_rate_df.movie_name2, how="inner").filter(col("Rating")==min(col("Rating"))).show()

print("Answer_6")
m_df=Movie_df.join(Movie_rate_df, on=Movie_df.movie_name1== Movie_rate_df.movie_name2, how="inner"). \
    filter("Rating BETWEEN 3 AND 8 And year1%4==0 ")
m_df.show()
m_df.groupby("movie_name1").count().filter(col("count")>2).show()

print("Answer_9")

Movie_rate_df.withColumn("len",length("movie_name2")).filter("Rating>len").show()

print("Answer_10")
Movie_rate_df.withColumn("len",length("movie_name2")).orderBy(col("movie_name2"),col("len"),col("Rating")).show()
