
import psycopg2
import os
import sys
from psycopg2 import sql
# Donot close the connection inside this file i.e. do not perform openconnection.close()
def RangeQuery(ratingMinValue, ratingMaxValue, openconnection, outputPath):
    #Implement RangeQuery Here.
    try:
        #opening the cursor
        cur = openconnection.cursor()

        #getting the number of round robin partition
        cur.execute("select * from RoundRobinRatingsMetadata")
        num_round_parts = cur.fetchone()[0]

        # Getting only the needed range partitions which will have the given range of ratings
        if(ratingMaxValue==0):
            cur.execute("select partitionnum from RangeRatingsMetadata "
                        "where minrating <= %s and maxrating>= %s", (ratingMaxValue, ratingMinValue))
        else:
            cur.execute("select partitionnum from RangeRatingsMetadata "
                    "where minrating < %s and maxrating>= %s",(ratingMaxValue,ratingMinValue))

        # creating list of all ratings table in which the given range exists
        ratings_parts = [i[0] for i in cur.fetchall()]

        if os.path.exists(outputPath):
            os.remove(outputPath)

        f = open(outputPath, "a+")

        for i in range(num_round_parts):
            tb_name = "RoundRobinRatingsPart" + str(i)
            # selecting relavent data
            cur.execute(sql.SQL("select * from {} "
                                "where Rating<= %s and Rating>= %s").format(sql.Identifier(tb_name.lower())),
                        [ratingMaxValue, ratingMinValue])
            quer = cur.fetchall()
            for row in quer:
                f.write("{},{},{},{}\n".format(tb_name, row[0], row[1], row[2]))


        for i in ratings_parts:

            tb_name = "RangeRatingsPart"+str(i)
            # selecting relavent data
            cur.execute(sql.SQL("select * from {} "
                        "where Rating<= %s and Rating>= %s").format(sql.Identifier(tb_name.lower())),[ratingMaxValue,ratingMinValue])
            quer = cur.fetchall()
            for row in quer:
                f.write("{},{},{},{}\n".format(tb_name,row[0],row[1],row[2]))

        f.close()

    finally:
        if cur:
            cur.close()


def PointQuery(ratingValue, openconnection, outputPath):
    #Implement PointQuery Here.
    try:
        #opening the cursor
        cur = openconnection.cursor()

        #getting the number of round robin partition
        cur.execute("select * from RoundRobinRatingsMetadata")
        num_round_parts = cur.fetchone()[0]


        # Getting only the needed range partitions which will have the given pointratings
        if(ratingValue==0):
            cur.execute("select partitionnum from RangeRatingsMetadata "
                        "where minrating <= %s and maxrating>= %s", (ratingValue, ratingValue))
        else:
            cur.execute("select partitionnum from RangeRatingsMetadata "
                    "where minrating < %s and maxrating>= %s",(ratingValue,ratingValue))
        ratings_part = cur.fetchone()[0]

        # If already exists, delete the file
        if os.path.exists(outputPath):
            os.remove(outputPath)

        # This will create and open the file in append mode
        f = open(outputPath, "a+")

        # iterate over each round robin partition and fetch the required data
        for i in range(num_round_parts):
            tb_name = "RoundRobinRatingsPart" + str(i)
            # selecting relavent data
            cur.execute(sql.SQL("select * from {} "
                                "where Rating= %s").format(sql.Identifier(tb_name.lower())),
                        [ratingValue,])
            quer = cur.fetchall()

            # Writing each row of the query into the file as per required format
            for row in quer:
                f.write("{},{},{},{}\n".format(tb_name, row[0], row[1], row[2]))

        # for ratings partition

        tb_name = "RangeRatingsPart"+str(ratings_part)
        # selecting relavent data
        cur.execute(sql.SQL("select * from {} "
                    "where Rating= %s").format(sql.Identifier(tb_name.lower())),[ratingValue,])
        quer = cur.fetchall()
        for row in quer:
            f.write("{},{},{},{}\n".format(tb_name,row[0],row[1],row[2]))

        f.close()

    finally:
        if cur:
            cur.close()