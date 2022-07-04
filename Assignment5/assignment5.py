#!/usr/local/bin/python3

"""
Big data computing assignment 5.

    Uses the PySpark dataframe functions to answer the following questions:
    1. How many distinct protein annotations are found in the dataset? I.e. how many distinc InterPRO numbers are there?
    2. How many annotations does a protein have on average?
    3. What is the most common GO Term found?
    4. What is the average size of an InterPRO feature found in the dataset?
    5. What is the top 10 most common InterPRO features?
    6. If you select InterPRO features that are almost the same size (within 90-100%) as the protein itself, what is the top10 then?
    7. If you look at those features which also have textual annotation, what is the top 10 most common word found in that annotation?
    8. And the top 10 least common?
    9. Combining your answers for Q6 and Q7, what are the 10 most commons words found for the largest InterPRO features?
    10. What is the coefficient of correlation ($R^2$) between the size of the protein and the number of features found?
"""

__author__ = "Linda de Vries"
__version__ = "1.0"

from csv import writer
import sys
import os

from pyspark.sql.types import StructType, StructField, IntegerType, StringType
from pyspark.sql.functions import mean, desc, split, col, explode, asc
from pyspark.sql import SparkSession
from pyspark import SparkContext


class InterProScanAnalysis():
    """ This class reads and manipulates the Interpro tsv file """

    def __init__(self, df):
        self.df = df
        self.results = []
        self.explains = []

    def question_1(self):
        """ Question one """
        amount_of_unique_interpro = self.df.select("_c11").distinct().count()
        explained_1 = self.df._jdf.queryExecution().toString()
        self.results.append(amount_of_unique_interpro)
        self.explains.append(explained_1)
        return amount_of_unique_interpro, explained_1

    def question_2(self):
        """ Question two """
        collected_data = self.df.groupBy("_c0").count().select(mean("count")).collect()
        average_annotations = collected_data[0][0]
        explained_2 = self.df._jdf.queryExecution().toString()
        self.results.append(average_annotations)
        self.explains.append(explained_2)
        return average_annotations, explained_2

    def question_3(self):
        """ Question three """
        exploded_df = self.df.withColumn('_c13', explode(split("_c13", "[|]")))
        grouped_df = exploded_df.groupby("_c13").count()
        most_common = grouped_df.orderBy(desc('count')).take(2)[1][0]
        explained_3 = grouped_df._jdf.queryExecution().toString()
        self.results.append(most_common)
        self.explains.append(explained_3)
        return most_common, explained_3

    def question_4(self):
        """ Question four """
        average_length = self.df.select(mean("_c2")).collect()
        explained_4 = self.df._jdf.queryExecution().toString()
        self.results.append(average_length[0][0])
        self.explains.append(explained_4)
        return average_length[0][0], explained_4

    def question_5(self, df):
        """ Question five """
        grouped_df = df.groupby("_c11").count()
        most_common = grouped_df.orderBy(desc('count')).take(11)
        top_10 = [x[0] for x in most_common if x[0] != "-"]
        explained_5 = grouped_df._jdf.queryExecution().toString()
        self.results.append(top_10)
        self.explains.append(explained_5)
        return top_10, explained_5

    def question_6(self):
        """ Question six """
        length_df = self.df.withColumn("Lenght", self.df['_c7'] - self.df['_c6'] + 1)
        range_df = length_df.withColumn("bottom_range", length_df["_c2"] * 0.9)
        similar_df = range_df.filter(
            (range_df["Lenght"] >= range_df["bottom_range"]) & (range_df["Lenght"] < range_df["_c2"]))
        return self.question_5(similar_df)

    def question_7(self):
        """ Question seven """
        exploded_df = self.df.withColumn('_c12', explode(split("_c12", ",")))
        grouped_df = exploded_df.groupby("_c12").count()
        most_common = grouped_df.orderBy(desc('count')).take(11)
        top_10 = [x[0] for x in most_common if x[0] != "-"]
        explained_7 = grouped_df._jdf.queryExecution().toString()
        self.results.append(top_10)
        self.explains.append(explained_7)
        return top_10, explained_7

    def question_8(self):
        """ Question eigth """
        exploded_df = self.df.withColumn('_c12', explode(split("_c12", ",")))
        grouped_df = exploded_df.groupby("_c12").count()
        most_common = grouped_df.orderBy(col("count").asc()).take(10)
        top_10 = [x[0] for x in most_common if x[0] != "-"]
        explained_8= grouped_df._jdf.queryExecution().toString()
        self.results.append(top_10)
        self.explains.append(explained_8)
        return top_10, explained_8

    def question_9(self):
        """ Question 9 """
        length_df = self.df.withColumn("Lenght", self.df['_c7'] - self.df['_c6'] + 1)
        range_df = length_df.withColumn("bottom_range", length_df["_c2"] * 0.9)
        similar_df = range_df.filter(
            (range_df["Lenght"] >= range_df["bottom_range"]) & (range_df["Lenght"] < range_df["_c2"]))
        exploded_df = similar_df.withColumn('_c12', explode(split("_c12", ",")))
        grouped_df = exploded_df.groupby("_c12").count()
        most_common = grouped_df.orderBy(desc('count')).take(11)
        top_10 = [x[0] for x in most_common if x[0] != "-"]
        explained_9 = grouped_df._jdf.queryExecution().toString()
        self.results.append(top_10)
        self.explains.append(explained_9)
        return top_10, explained_9

    def question_10(self):
        """" Question 10 """
        subset_df = self.df.select(["_c0", "_c2", "_c11"])
        subset_df = subset_df.filter((self.df["_c11"] != None) | (self.df["_c11"] != "-"))
        subset_df = subset_df.groupBy("_c0", "_c2").count()
        subset_df = subset_df.withColumn("_c2", subset_df["_c2"].cast(IntegerType()))
        subset_df = subset_df.withColumn("count", subset_df["count"].cast(IntegerType()))
        explained_10 = subset_df._jdf.queryExecution().toString()
        subs = subset_df.stat.corr("_c2", "count")
        self.results.append(subs)
        self.explains.append(explained_10)
        print(subs)
        return subs, explained_10

    def write_interpro_csv(self):
        """ Writes the output to a csv file """
        print(self.results)
        with open('output.csv', 'a', newline='') as f_object:
            for i, result, explains in zip([i for i in range(1, 11)], self.results, self.explains):
                writer_object = writer(f_object)
                writer_object.writerow([i, result, explains])
            f_object.close()


def main(args):
    sc = SparkContext('local[16]')
    spark = SparkSession \
        .builder \
        .appName("how to read csv file") \
        .getOrCreate()

    path = args[1]
    print(os.getcwd())
    df = spark.read.csv(path, sep='\t', header=False)
    interparser = InterProScanAnalysis(df)
    df.show()
    res1, explained_1 = interparser.question_1()
    res2, explained_2 = interparser.question_2()
    res3, explained_3 = interparser.question_3()
    res4, explained_4 = interparser.question_4()
    res5, explained_5 = interparser.question_5(df)
    res6, explained_6 = interparser.question_6()
    res7, explained_7 = interparser.question_7()
    res8, explained_9 = interparser.question_8()
    res9, explained_9 = interparser.question_9()
    res10, explained_10 = interparser.question_10()

    # Write outputs to csv
    interparser.write_interpro_csv()


if __name__ == "__main__":
    sys.exit(main(sys.argv))
