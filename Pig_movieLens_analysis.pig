
/*+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++*/
/*-----------------------------------SETUP---------------------------------------*/
/*+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++*/

-- Ensure the 'dump' command will run on mapreduce mode
set opt.fetch false



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

-- Find the most rated movie of all movies where the movies average rating is 4.6 or above
most_liked_movie = FOREACH (GROUP average_movie_ratings ALL) {
	filtered = FILTER average_movie_ratings BY rating_average >= (float)4.6;
	ordered_movies = ORDER filtered BY num_ratings DESC;
	most_rated_movie_with_specified_avg = LIMIT ordered_movies 1;
    GENERATE FLATTEN(most_rated_movie_with_specified_avg);
}

-- Find the multiple most rated movies of all movies where the movies average rating is 4.0 stars or above
most_liked_movies = FOREACH (GROUP average_movie_ratings ALL) {
	filtered = FILTER average_movie_ratings BY (rating_average >= (float)4.66) AND (num_ratings == (int)6);
    GENERATE FLATTEN(filtered);
}



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
