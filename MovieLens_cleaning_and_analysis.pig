
-- describe data: http://files.grouplens.org/datasets/movielens/ml-latest-small-README.html



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



/*+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++*/
/*------------------------------FULL PIG ANALYSIS--------------------------------*/
/*+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++*/


/*=====================Most_Rated_Movie==================*/

-- Read back in the joined cleaned up movie and rating file with the pipe as a delimiter
cleaned_movies_nd_ratings = LOAD 'output/cleaned_movies_nd_ratings' using PigStorage('|') AS (movieId:int, title:chararray, year:chararray, genres:chararray, userId:int, rating:int, timestamp:int);

-- Count the number ratings each movie has
group_movies = GROUP cleaned_movies_nd_ratings BY (movieId, title);
count_movie_ratings = FOREACH group_movies GENERATE group.movieId, group.title, COUNT(cleaned_movies_nd_ratings) as num_ratings;

-- Find the movie with the most ratings
top_movie_rating = FOREACH (GROUP count_movie_ratings ALL) {
    ordered = ORDER count_movie_ratings BY num_ratings DESC;
    limited = LIMIT ordered 1;
    GENERATE FLATTEN(limited);
}

dump top_movie_rating

/*=====================Most_Liked_Movie==================*/

-- Read back in the joined cleaned up movie and rating file with the pipe as a delimiter
cleaned_movies_nd_ratings = LOAD 'output/cleaned_movies_nd_ratings' using PigStorage('|') AS (movieId:int, title:chararray, year:chararray, genres:chararray, userId:int, rating:int, timestamp:int);

-- Count the number of each rating each movie has
group_movie_and_its_rating = GROUP cleaned_movies_nd_ratings BY (movieId, title, rating);
count_num_each_rating_for_each_movie = FOREACH group_movie_and_its_rating generate group.movieId, group.title, group.rating, COUNT(cleaned_movies_nd_ratings) as num_ratings;

-- Group the ratings together by their movie
group_movies = GROUP count_num_each_rating_for_each_movie BY (movieId, title);

-- Calculate the average rating for each movie
average_movie_ratings = FOREACH group_movies {
	rating_times_num_of_ratings = FOREACH count_num_each_rating_for_each_movie GENERATE rating * num_ratings;
    GENERATE group.movieId, group.title, SUM(count_num_each_rating_for_each_movie.num_ratings) AS num_ratings, (float)SUM(rating_times_num_of_ratings) / SUM(count_num_each_rating_for_each_movie.num_ratings) AS rating_average;
}

-- Find the most rated movie of all movies where the movies average rating is 4-stars or above
most_liked_movie = FOREACH (GROUP average_movie_ratings ALL) {
	filtered = FILTER average_movie_ratings BY rating_average >= (float)4.0;
	ordered_movies = ORDER filtered BY num_ratings DESC;
	most_rated_movie_with_specified_avg = LIMIT ordered_movies 1;
    GENERATE FLATTEN(most_rated_movie_with_specified_avg);
}

dump most_liked_movie

-- Find the multiple most rated movies of all movies where the movies average rating is 4.0 stars or above
most_liked_movies = FOREACH (GROUP average_movie_ratings ALL) {
	filtered = FILTER average_movie_ratings BY (rating_average >= (float)4.0) AND (num_ratings == (int)6);
    GENERATE FLATTEN(filtered);
}

dump most_liked_movies


/*=====================User_With_Highest_Average_Rating==================*/

-- Read back in the joined cleaned up movie and rating file with the pipe as a delimiter
cleaned_movies_nd_ratings = LOAD 'output/cleaned_movies_nd_ratings' using PigStorage('|') AS (movieId:int, title:chararray, year:chararray, genres:chararray, userId:int, rating:int, timestamp:int);

-- Group the data by each user
group_users = GROUP cleaned_movies_nd_ratings BY userId;

-- Get the average rating for each user
average_user_rating = FOREACH group_users GENERATE group As userId, SUM(cleaned_movies_nd_ratings.rating) as num_ratings, AVG(cleaned_movies_nd_ratings.rating) AS average_rating;

-- Get the users with 5-star rating who has the most number of ratings places
user_highest_avg = FOREACH (GROUP average_user_rating ALL) {
	filtered = FILTER average_user_rating BY average_rating == (float)5.0;
	ordered = ORDER filtered by num_ratings DESC;
	limited = LIMIT ordered 1;
	GENERATE FLATTEN(limited);
}

dump user_highest_avg

-- See if anybody else has 100 ratings placed and a 5-star average
multiple_users_highest_avg = FOREACH (GROUP average_user_rating ALL) {
	filtered = FILTER average_user_rating BY (average_rating == (float)5.0) AND (num_ratings > 99);
	GENERATE FLATTEN(filtered);
}

dump multiple_users_highest_avg
