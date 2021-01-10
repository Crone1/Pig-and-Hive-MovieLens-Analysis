# Overview
This repository contains the my work in analysing the MovieLens dataset using the big data tools Pig and Hive for the module 'Data Analysis at Speed and Scale'.

A description of the MovieLens data can be seen at http://files.grouplens.org/datasets/movielens/ml-latest-small-README.html

In this work, I perform analysis and ran queries on this dataset in both Pig and Hive, however, I first needed to clean the data in PIG.

I used google cloud platform for this analysis. I found this much easier and more reliable than having the cluster hosted locally on my computer.

# Obtaining the data
Before I could clean the data or perform analysis on it, I first had to obtain it and put it onto my HDFS cluster.
This was done using the following commands:

      -- Get the data files
      wget http://files.grouplens.org/datasets/movielens/ml-latest-small.zip

      -- Unzip the folder with this data in it
      unzip ml-latest-small.zip

      -- Take the NameNode out of safemode
      sudo -u hdfs hdfs dfsadmin -safemode leave

      -- Make a directory on the hdfs with my username
      hadoop fs -mkdir /user/Owner

      -- Put the data onto the HDFS in the folder with my username
      hadoop fs -put ml-latest-small /user/Owner/


# Breakdown of the code
After these steps, I knew I had the data on the HDFS and could read it into PIG from there. The code in the *'Pig_movieLens_cleaning.pig'* file can then be run bit by bit to clean the data and then the code in the *'Pig_movieLens_analysis.pig'* file can be run to perform the analysis. This is further broke down in the notebooks using comments.

The *'Hive_movielens_analysis.hive'* file then further explores this dataset to derive more insights.
