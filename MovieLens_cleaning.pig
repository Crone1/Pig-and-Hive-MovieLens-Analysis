
/*+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++*/
/*-----------------------------------SETUP---------------------------------------*/
/*+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++*/

-- Ensure the 'dump' command will run on mapreduce mode
set opt.fetch false



/*+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++*/
/*------------------------READ IN MOVIE DATA & CLEAN-----------------------------*/
/*+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++*/

-- Read in the data
DEFINE CSVLoader org.apache.pig.piggybank.storage.CSVLoader();
read_movies = LOAD 'ml-latest-small/movies.csv' using CSVLoader() AS (movieId:int, title:chararray, genres:chararray);

-- Filter out the first row of titles
movies = FILTER read_movies BY (SUBSTRING(title, 0, 5) != 'title') AND (SUBSTRING(genres, 0, 6) != 'genres');

-- Split out genres into multiple columns (ideally have a column for each genre)
fix_movie_genres = FOREACH movies GENERATE movieId, title, STRSPLIT(genres, '\\|') AS genres;

-- replace comma(,) if it appears in title
replace_movie_title_comma = FOREACH fix_movie_genres GENERATE movieId AS movieId, REPLACE (title, ',', '@@@') AS title, genres AS genres;

-- fix the delimeter, change commas to pipe and output this to a file
STORE replace_movie_title_comma INTO 'output/piped_movies' using PigStorage('|');

-- Read back in this file with the pipe as a delimiter
movies_with_pipe = LOAD 'output/piped_movies' using PigStorage('|') AS (movieId:int, title:chararray, genres:chararray);

-- replace '@@@' back to comma(,) if it appears in title 
part_cleaned_movies = FOREACH movies_with_pipe GENERATE movieId AS movieId, REPLACE (title, '@@@', ',') AS title, genres AS genres;

-- Split up the year from the title
fully_cleaned_movies = FOREACH part_cleaned_movies GENERATE
    movieId,
    REGEX_EXTRACT(title, '([\\S ]+) \\((\\d{4}|\\d{4}-?\\d{4})\\)', 1) AS title,
    REGEX_EXTRACT(title, '\\((\\d{4}|(\\d{4}-\\d{4}))\\)', 1) AS year,
    genres;

-- Store this cleaned movies file for later use
STORE fully_cleaned_movies INTO 'output/cleaned_movies' using PigStorage('|');



/*+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++*/
/*------------------------READ IN RATINGS DATA & CLEAN---------------------------*/
/*+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++*/

-- Read in the data
read_ratings = LOAD 'ml-latest-small/ratings.csv' using PigStorage(',') AS (userId:int, movieId:int, rating:int, timestamp:int);

-- Filter out the first row as it is empty
comma_del_ratings = FILTER read_ratings BY (userId IS NOT NULL) AND (movieId IS NOT NULL);

-- fix the delimeter, change commas to pipe and output this to a file
STORE comma_del_ratings INTO 'output/piped_ratings' using PigStorage('|');



/*+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++*/
/*-----------------------JOIN INTO ONE TABLE-------------------------------------*/
/*+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++*/

-- Read back in the cleaned_movies file with the pipe as a delimiter
cleaned_movies = LOAD 'output/cleaned_movies' using PigStorage('|') AS (movieId:int, title:chararray, year:chararray, genres:chararray);

-- Read back in this file with the pipe as a delimiter
ratings = LOAD 'output/piped_ratings' using PigStorage('|') AS (userId:int, movieId:int, rating:int, timestamp:int);

-- Join the ratings table and the cleaned_movies table
joined_m_nd_r = JOIN cleaned_movies BY movieId, ratings BY movieId;
joined_movies_and_ratings = FOREACH joined_m_nd_r GENERATE cleaned_movies::movieId AS movieId, cleaned_movies::title AS title, cleaned_movies::year AS year, cleaned_movies::genres AS genres, ratings::userId AS userId, ratings::rating AS rating, ratings::timestamp AS timestamp;

join_limit = LIMIT joined_movies_and_ratings 10;

-- Store this file away for use later on in analysis
fs -rm -r output/cleaned_movies_nd_ratings
STORE joined_movies_and_ratings INTO 'output/cleaned_movies_nd_ratings' using PigStorage('|');
