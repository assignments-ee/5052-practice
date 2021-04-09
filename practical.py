#!/usr/bin/env python
# coding: utf-8

# In[ ]:





# In[15]:


import findspark
findspark.init()
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import collect_list
import pickle
import sys


spark = SparkSession.builder.master('local').appName("practical").config("conf-key", "conf-value").getOrCreate()
ratings = spark.read.option('inferSchema','true').option('header','true').csv('ml-latest-small/ratings.csv')
#ratings.printSchema()
#ratings.show()
movies_info = spark.read.option('inferSchema','true').option('header','true').csv('ml-latest-small/movies.csv')


def search_movienum_byid(userId):
    users_sta = ratings.groupby('userId').agg({"movieId":"count"})
    
    users_rdd = users_sta.rdd
    users_tuple = users_rdd.map(lambda x: (x[0], x[1])).collect()
    #list_users = map(lambda row: row.asDict(), users_sta.collect())
    for i in users_tuple:
        if userId == i[0]:
            #print(i[1])
            #pickle.dump(i[1], 'result1.txt')
            with open('result1', 'w') as f:
                f.write(str(i[1]))
            

def search_movielist_byid(userId):
    movies_df = ratings.groupBy('userId').agg(collect_list("movieId"))
    movies_df.printSchema()
    #movies_df.show()
    movies_rdd = movies_df.rdd
    
    user_movies = movies_rdd.map(lambda x: (x[0], x[1])).collect()
    #print(user_movies)
    movies_name = movies_info.rdd

    movies_list = movies_name.map(lambda x: (x[0], x[1], x[2])).collect() #extract the id,name,and genre of movie into tuple
    
    for i in range(len(user_movies)):
        for j in range(len(user_movies[i][1])):
            for m in range(len(movies_list)): #search the same movie id in movies_list
                if user_movies[i][1][j] == movies_list[m][0]:
                    user_movies[i][1][j] = movies_list[m] #show the id,name and genre of movies in the list instead of just ids
    #print(user_movies)
    for user_movie in user_movies:
        if user_movie[0] == userId:
            #print(user_movie[1])
            
            import codecs
            f = codecs.open('result2.txt','w','utf-8')
            
            f.write(str(user_movie[1]))
            f.close()
#     for i in range(len(movies_name)):
#         movie_list.append(movies_name[i][0])
#     print(movie_list)
    
#     movies_name = movies_name.filter(lambda x: x[0]).collect()
#     for i in movies_name:
#         print(i)

def search_by_movieid(movieId):
    movies_sta = ratings.groupby('movieId').agg({"userId":"count"})
    movies_rdd = movies_sta.rdd
    movies_list = movies_rdd.map(lambda x: (x[0], x[1])).collect()
    #print(movies_list)
    
    movies_ave = ratings.groupby('movieId').agg({"rating":"avg"})
    rating_list = movies_ave.rdd.map(lambda x: (x[0], x[1])).collect()
    #print(rating_list)
    
    for tuple in movies_list:
        if tuple[0] == movieId:
            print(str(tuple[1]) + " users have watched this movie")
            import codecs
            f = codecs.open('result3.txt','w','utf-8')
            
            f.write(str(tuple[1]) + " users have watched this movie")
            f.close()
    for id_averate in rating_list:
        if id_averate[0] == movieId:
            #print('the average rating of this movie is ' + str(id_averate[1]))
            import codecs
            f = codecs.open('result3.txt','a','utf-8')
            
            f.write('the average rating of this movie is ' + str(id_averate[1]))
            f.close()

def search_by_genre(genre):
    genres_df = movies_info.groupBy('genres').agg(collect_list("movieId"))
    genres_list = genres_df.rdd.map(lambda x: (x[0], x[1])).collect()
    #print(genres_list)
    id_titles = movies_info.select('movieId', 'title')
    id_titles_list = id_titles.rdd.map(lambda x: (x[0], x[1])).collect()
    #print(id_titles_list)
    for i in range(len(genres_list)):
        for j in range(len(genres_list[i][1])):
            for m in range(len(id_titles_list)):
                if genres_list[i][1][j] == id_titles_list[m][0]:
                    genres_list[i][1][j] = id_titles_list[m]
    #print(genres_list)
    for tuple in genres_list:
        if tuple[0] == genre:
            #print(tuple[1])
            import codecs
            f = codecs.open('result4.txt','w','utf-8')
            
            f.write(str(tuple[1]))
            f.close()

def search_movie_byyear(year):
    movie_year_df = movies_info.select('title')
    year_list = movie_year_df.rdd.map(lambda x: (x[0], x[0].split(" ")[-1])).collect()
    #print(year_list)
    #print(movie_year_df.rdd.collect())
    #year_df = movies_info.groupBy('genres').agg(collect_list("movieId"))
    separ_df = spark.createDataFrame(year_list, ['title', 'year'])
    year_df = separ_df.groupBy('year').agg(collect_list("title"))
    year_list = year_df.rdd.map(lambda x: (x[0], x[1])).collect()
    #print(year_list)
    for tuple in year_list:
        if tuple[0] == '('+str(year)+')':
            #print(tuple[1])
            import codecs
            f = codecs.open('result5.txt','w','utf-8')
            
            f.write(str(tuple[1]))
            f.close()

def top_rating(n):
    
    rating_df = ratings.orderBy("rating", ascending=False)
    #rating_df.show()
    rating_df = rating_df.select('movieId', 'rating')
    topn_list= rating_df.rdd.map(lambda x: (x[0], x[1])).collect()
    #print(topn_list)
    topn_list = topn_list[:n]
    for index in range(len(topn_list)):
        topn_list[index] = list(topn_list[index])
    #print(topn_list)
    movie_ids = movies_info.select('movieId', 'title').rdd.map(lambda x: (x[0], x[1])).collect()
    #print(movie_ids)
    for i in range(len(topn_list)):
        for j in range(len(movie_ids)):
            if topn_list[i][0] == movie_ids[j][0]:
                topn_list[i][0] = movie_ids[j]
    #print(topn_list)
    
    import codecs
    f = codecs.open('result6.txt','w','utf-8')
            
    f.write(str(topn_list))
    f.close()



    
def top_watches(n):
    movies_df = ratings.groupby('movieId').agg({"movieId":"count"})
    movies_df = movies_df.orderBy("count(movieId)", ascending=False)
    #movies_df.show()
    watches_list = movies_df.rdd.map(lambda x: (x[0], x[1])).collect()
    watches_list = watches_list[:n]
    for index in range(len(watches_list)):
        watches_list[index] = list(watches_list[index])
    #print(watches_list)
    movie_ids = movies_info.select('movieId', 'title').rdd.map(lambda x: (x[0], x[1])).collect()
    for i in range(len(watches_list)):
        for j in range(len(movie_ids)):
            if watches_list[i][0] == movie_ids[j][0]:
                watches_list[i][0] = movie_ids[j]
    #print(watches_list)
    import codecs
    f = codecs.open('result7.txt','w','utf-8')
            
    f.write(str(watches_list))
    f.close()
    


def main(argv):
    if argv[1] == 'function1':
        search_movienum_byid(int(argv[2]))
    elif argv[1] == 'function2':
        search_movielist_byid(int(argv[2]))
    elif argv[1] == 'function3':
        search_by_movieid(int(argv[2]))
    elif argv[1] == 'function4':
        search_by_genre(argv[2])
    elif argv[1] == 'function5':
        search_movie_byyear(int(argv[2]))
    elif argv[1] == 'function6':
        top_rating(int(argv[2]))
    elif argv[1] == 'function7':
        top_watches(int(argv[2]))
    
    
    
    
if __name__ == '__main__':
    #search_movienum_byid(104)
    #search_movielist_byid(104)
    #search_by_movieid(47)
    #search_by_genre('Comedy|Romance')
    #search_movie_byyear(2017)
    #top_rating(10)
    #top_watches(3)
    main(sys.argv)


# In[ ]:





# In[ ]:




