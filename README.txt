 this file is used to show how to run the code about part1 and part2

1. we create two methods to realize the functions in parts1:

1.1 method1:
the solution2.py file realize the first method to deal with problems in part1

(1-1) Search user by id, show the number of movies/genre that he/she has watched
python3 solution2.py function_1_1 "1" "small"
(1-2) Given a list of users, search all movies watched by each user
python3 solution2.py function_1_2 "1,2,3" "small"
(2-1) Search movie by id, show the average rating, the number of users that have watched the movie
python3 solution2.py function_2_1 "1" "small"
(2-2) Search movie by title, show the average rating, the number of users that have watched the movie
python3 solution2.py function_2_2 "Toy Story" "small"
(3-1) Search genre, show all movies in that genre
python3 solution2.py function_3_1 "Adventure" "small"
(3-2) Given a list of genres, search all movies belonging to each genre
python3 solution2.py function_3_2 "Adventure,Romance" "small"
(4) Search movies by year
python3 solution2.py function_4 "1995" "small"
(5) List the top n movies with highest rating, ordered by the rating
python3 solution2.py function_5 "30" "small"
(6) List the top n movies with the highest number of watches, ordered by the number of watches
python3 solution2.py function_6 "30" "small"


1.2 method2:
the practical.py file realize the second method to deal with problems in part1
there is on point that you should pay attention to, the input of function2 and function3 should be a list of ids,but this method only take one id into consideration insteat of 
a list of ids. fortunately, the first method can successfully realize these two functions.

1).if you want to search user by id and get the number of moviesthat he/she has watched,
you can use the following command(suppose the userId is 3):
python3 practical.py function1 3

2). if you want to type a userId and get all movies watched by this user,
 you can use the following comand(suppose the userId is 3)
python3 practical.py function2 3

3). if you want to search movie by id, and get the average rating, the number of users that have watched
 the movie, you can run the following command(suppose the movieId is 3)
python3 practical.py function3 3

4) if you want to search genre, get all movies in that genre,you can run the following command(suppose the
genre name is 'Comedy|Romance')
python3 practical.py function4 'Comedy|Romance'

5). if you want to Search movies by year(suppose the year is 2017), you can use the following command:
python3 practical.py function5 2017

6).if you want to list the top n movies with highest rating(suppose we want to get top 5 movies), you can use the
following command:
python3 practical.py function6 5

7). if you want to get the top n movies with the highest number of watches(suppose top 5), you can use the following
command:
python3 practical.py function7 5


2. the methods to realize functions in part2 are shown as followed
the solution2.py file realize the method to deal with problems in part2

Part 2
(7) Find the favourite genre of a given user, or group of users.
python3 solution2.py function_7 "1,2,3" "small"
(8) Compare the movie tastes of two users.
python3 solution2.py function_8 "1,2" "small"
