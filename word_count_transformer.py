import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import collect_list, udf, col, concat_ws, expr
from collections import Counter
from pyspark.sql.types import StringType
spark = SparkSession.builder.getOrCreate()
input_path = "resources/word_count/words.txt"


# def run(spark: SparkSession, input_path: str, output_path: str) -> None:
#     logging.info("Reading text file from: %s", input_path)
#     input_df = spark.read.text(input_path)

#     input_df.show()

#     all_lines = input_df.select(collect_list("value")).first()[0]

#     #all_lines = 

#     all_words = [x.lower().split() for x in all_lines if len(x.split())]

#     all_words_flat = [x for i in all_words for x in i]

#     #print(all_words_flat)

#     word_count = dict(Counter(all_words_flat))

#     input_df = spark.createDataFrame(word_count.items(), "word: string, value: int")

#     input_df.show()

#     logging.info("Writing csv to directory: %s", output_path)

#     input_df.coalesce(1).write.mode("overwrite").csv(output_path, header=True)

#     return input_df

# def split_words(column):
#     return None

# split_words_udf = udf(lambda x: split_words(x), StringType())

# def split_transform(df):
#     return df.withColumn("word", split_words_udf)

def run(spark: SparkSession, input_path: str, output_path: str) -> None:
    logging.info("Reading text file from: %s", input_path)
    input_df = spark.read.text(input_path)

    input_df.createOrReplaceTempView("words")

    input_df = spark.sql("select split(value, ' ') as split_words from words")

    input_df = input_df.withColumn("word", concat_ws(",", col("split_words")))

    join_expression = "+".join(input_df.columns)

    input_df = input_df.withColumn("all_words", expr(join_expression))

    input_df.show()

    return input_df

run(spark, input_path, "test_output")