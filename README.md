This repository contains the college work I did during the module 'Data Analysis at Speed and Scale' using the big data tool - Pig.

In this work, I used the MovieLens datast to perform analysis on and run some queries.
This involved first cleaning the data and then running the queries in PIG.

A description of this data can be seen at http://files.grouplens.org/datasets/movielens/ml-latest-small-README.html

Before I could clean the data or perform analysis on it, I first had to obtain it and put it onto my HDFS cluster. This was done using the following commands:

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
      
After these steps, I knew I had the data on the HDFS and could read it into PIG from there.

Follow on analysis can be seen in my Hive repository. This repository took the cleaned data from PIG and used this to perform alternate analysis queries. This Hive repository can be found at https://github.com/Crone1/Hive
