from pyspark.sql import SparkSession
from pyspark.sql import Row
import sys

if len(sys.argv) != 4:
    print('''Usage: python3 solution2.py <function_name> <param2> <file_size>
                    <function_name> example: function_1_1
                    <param2> example: 1
                    <file_size> example: whole/small
              Concrete parameter information please refer to the report of this coursework''')
    sys.exit(-1)

function_name = sys.argv[1]
param2 = sys.argv[2]
file_size = sys.argv[3]
file_path = ''

if file_size == 'whole':
    file_path = 'ml-latest'
elif file_size == 'small':
    file_path = 'ml-latest-small'

spark = SparkSession.builder.master('local').appName("practical").getOrCreate()

links = spark.read.option('inferSchema','true').option('header','true').csv(file_path + '/links.csv')
#links.printSchema()
#links.show()

movies = spark.read.option('inferSchema','true').option('header','true').csv(file_path + '/movies.csv')
#movies.printSchema()
#movies.show()

ratings = spark.read.option('inferSchema','true').option('header','true').csv(file_path + '/ratings.csv')
#ratings.printSchema()
#ratings.show()

tags = spark.read.option('inferSchema','true').option('header','true').csv(file_path + '/tags.csv')
#tags.printSchema()
#tags.show()

links.createOrReplaceTempView("links")
movies.createOrReplaceTempView("movies")
ratings.createOrReplaceTempView("ratings")
tags.createOrReplaceTempView("tags")

def function_1_1(param2):
    userId = param2
    sql_1_1 = '''SELECT t1.userId, count(distinct t1.movieid), count(distinct t2.genres)
               FROM ratings t1, movies t2
               WHERE t1.movieId = t2.movieId and t1.userId = ''' + userId + '''
               GROUP BY t1.userId'''
    function_1_1 = spark.sql(sql_1_1)
    function_1_1.show()
    function_1_1.coalesce(1).write.option("header", "true").csv('function_1_1')
    print("Result csv file has been saved to: " + function_name)


def function_1_2(param2):
    users = param2

    sql_1_2 = '''SELECT t1.userId, count(distinct t1.movieid), count(distinct t2.genres)
               FROM ratings t1, movies t2
               WHERE t1.movieId = t2.movieId and t1.userId in ( ''' + users +''')
               GROUP BY t1.userid
               ORDER BY t1.userid '''
    function_1_2 = spark.sql(sql_1_2)
    function_1_2.show()
    function_1_2.coalesce(1).write.option("header", "true").csv('function_1_2')
    print("Result csv file has been saved to: " + function_name)


def function_2_1(param2):
    movieId = param2

    sql_2_1 = '''SELECT t1.movieId, avg(t1.rating), count(distinct t1.userId)
               FROM ratings t1
               WHERE t1.movieId = ''' + movieId + '''
               GROUP BY t1.movieId'''
    function_2_1 = spark.sql(sql_2_1)
    function_2_1.show()
    function_2_1.coalesce(1).write.option("header", "true").csv('function_2_1')
    print("Result csv file has been saved to: " + function_name)


def function_2_2(param2):
    title = param2
    sql_2_2 = '''SELECT t2.title, avg(t1.rating), count(distinct t1.userId)
               FROM ratings t1, movies t2
               WHERE t2.title LIKE \'%'''+ title +'''%\' AND t1.movieId = t2.movieId
               GROUP BY t2.title'''
    function_2_2 = spark.sql(sql_2_2)
    function_2_2.show()
    function_2_2.coalesce(1).write.option("header", "true").csv('function_2_2')
    print("Result csv file has been saved to: " + function_name)


def function_3_1(param2):
    genre = param2
    sql_3_1 = '''SELECT t1.genres, t1.title
               FROM movies t1
               WHERE t1.genres LIKE '%'''+ genre +'''%'
               ORDER BY t1.genres'''
    function_3_1 = spark.sql(sql_3_1)
    function_3_1.show()
    function_3_1.coalesce(1).write.option("header", "true").csv('function_3_1')
    print("Result csv file has been saved to: " + function_name)


def function_3_2(param2):
    genres = param2
    genre_list = genres.split(',')
    #print(genre_list)

    for i in genre_list:
        sql_3_2 = '''SELECT t1.genres, t1.title
                   FROM movies t1
                   WHERE t1.genres LIKE '%'''+ i +'''%'
                   ORDER BY t1.genres'''
        function_3_2 = spark.sql(sql_3_2)
        function_3_2.show()
        function_3_2.coalesce(1).write.option("header", "true").csv('function_3_2_' + i)
        print("Result csv file has been saved to: " + function_name + '_' + i)


def function_4(param2):
    year = param2

    sql_4 = '''SELECT t1.title
               FROM movies t1
               WHERE t1.title LIKE '%'''+ year +'%\''
    function_4 = spark.sql(sql_4)
    function_4.show()

    function_4.coalesce(1).write.option("header", "true").csv('function_4')
    print("Result csv file has been saved to: " + function_name)


def function_5(param2):
    num = int(param2)
    sql_5 = '''SELECT DISTINCT t1.movieId, t2.title, t1.rating
               FROM ratings t1, movies t2
               WHERE t1.movieid = t2.movieId
               ORDER BY t1.rating desc'''
    function_5_all = spark.sql(sql_5)
    function_5 = function_5_all.head(num)

    function_5 = spark.createDataFrame(function_5)
    function_5.show()
    function_5.coalesce(1).write.option("header", "true").csv('function_5')
    print("Result csv file has been saved to: " + function_name)


def function_6(param2):
    num = int(param2)
    sql_6 = '''SELECT t2.movieId, t2.title, sum(distinct t1.userId)
               FROM ratings t1, movies t2
               WHERE t1.movieid = t2.movieId
               GROUP BY t2.movieId, t2.title
               ORDER BY sum(t1.userId) desc'''
    function_6_all = spark.sql(sql_6)
    function_6 = function_6_all.head(num)

    function_6 = spark.createDataFrame(function_6)
    function_6.show()
    function_6.coalesce(1).write.option("header", "true").csv('function_6')
    print("Result csv file has been saved to: " + function_name)


def function_7(param2):
    userId = param2
    user_list = userId.split(',')

    for i in user_list:
        sql_7 = '''SELECT t2.genres, count(t2.genres)
                   FROM ratings t1, movies t2
                   WHERE t1.movieid = t2.movieId AND t1.userId = '''+ i +'''
                   GROUP BY t2.genres
                   ORDER BY count(t2.genres) DESC'''
        function_7_all = spark.sql(sql_7)
        function_7_first = function_7_all.head(1)
        function_7_first = spark.createDataFrame(function_7_first)
        function_7_first.show()
        function_7_first.coalesce(1).write.option("header", "true").csv('function_7_fav_of_user' + i)
        print("Result csv file has been saved to: function_7_fav_of_user" + i)


def function_8(param2):
    two_users = param2
    two_users_list = two_users.split(',')
    userId_1 = two_users_list[0]
    userId_2 = two_users_list[1]

    sql_8_1 = '''SELECT t2.genres, count(t2.genres)
                   FROM ratings t1, movies t2
                   WHERE t1.movieid = t2.movieId AND t1.userId = '''+ userId_1 +'''
                   GROUP BY t2.genres
                   ORDER BY count(t2.genres) DESC'''
    function_8_1_all = spark.sql(sql_8_1)
    function_8_1_first = function_8_1_all.head(1)
    #function_8_1_first

    sql_8_2 = '''SELECT t2.genres, count(t2.genres)
                   FROM ratings t1, movies t2
                   WHERE t1.movieid = t2.movieId AND t1.userId = '''+ userId_2 +'''
                   GROUP BY t2.genres
                   ORDER BY count(t2.genres) DESC'''
    function_8_2_all = spark.sql(sql_8_2)
    function_8_2_first = function_8_2_all.head(1)
    #function_8_2_first

    user_1_fav = function_8_1_first[0].genres
    user_2_fav = function_8_2_first[0].genres

    comparison = [Row(userId_1, user_1_fav), Row(userId_2, user_2_fav)]
    comparison_df = spark.createDataFrame(comparison, ['userId', 'user_favourite_genre'])
    comparison_df.show()
    comparison_df.coalesce(1).write.option("header", "true").csv('function_8_comparison_of_users')
    print("Result csv file has been saved to: " + function_name)


if __name__ == '__main__':

    if function_name == 'function_1_1':
        function_1_1(param2)
    elif function_name == 'function_1_2':
        function_1_2(param2)
    elif function_name == 'function_2_1':
        function_2_1(param2)
    elif function_name == 'function_2_2':
        function_2_2(param2)
    elif function_name == 'function_3_1':
        function_3_1(param2)
    elif function_name == 'function_3_2':
        function_3_2(param2)
    elif function_name == 'function_4':
        function_4(param2)
    elif function_name == 'function_5':
        function_5(param2)
    elif function_name == 'function_6':
        function_6(param2)
    elif function_name == 'function_7':
        function_7(param2)
    elif function_name == 'function_8':
        function_8(param2)
