import psycopg2 as pg2
import pandas as pd

conn = pg2.connect(host='localhost',user='postgres',password='karthi123',port=5432,database='YoutubeData')
if conn:
    print("Connection Established Successfully")
    database = conn.cursor()
def question1():   
    database.execute("select channel.channel_name, video.video_name from channel inner join video on channel.channel_name = video.channel_name;")
    result = database.fetchall()
    ans1 = pd.DataFrame(result, columns=['channel_name', 'video_name']).reset_index(drop=True)
    ans1.index += 1
    return ans1

def question2():   
    database.execute("SELECT channel.channel_name, COUNT(video.video_id) AS video_count FROM channel JOIN video ON channel.channel_name = video.channel_name GROUP BY channel.channel_name ORDER BY video_count DESC;")
    result = database.fetchall()
    ans1 = pd.DataFrame(result, columns=['channel.channel_name', 'video_count']).reset_index(drop=True)
    ans1.index += 1
    return ans1

def question3():   
    database.execute("SELECT video.video_name, channel.channel_name, video.view_count FROM video JOIN channel ON video.channel_name = channel.channel_name ORDER BY video.view_count DESC LIMIT 10;")
    result = database.fetchall()
    ans1 = pd.DataFrame(result, columns=['video.video_name', 'channel.channel_name','video.view_count']).reset_index(drop=True)
    ans1.index += 1
    return ans1

def question4():   
    database.execute("SELECT video_name, comment_count from video ORDER BY comment_count DESC;")
    result = database.fetchall()
    ans1 = pd.DataFrame(result, columns=['video_name', 'comment_count']).reset_index(drop=True)
    ans1.index += 1
    return ans1

def question5():   
    database.execute("SELECT video.video_name, channel.channel_name, video.like_count FROM video JOIN channel ON video.channel_name = channel.channel_name ORDER BY video.like_count DESC;")
    result = database.fetchall()
    ans1 = pd.DataFrame(result, columns=['video.video_name', 'channel.channel_name','video.like_count']).reset_index(drop=True)
    ans1.index += 1
    return ans1

def question6():   
    database.execute("SELECT video_name, like_count, dislike_count FROM video ORDER BY like_count DESC, dislike_count ASC;")
    result = database.fetchall()
    ans1 = pd.DataFrame(result, columns=['video_name', 'like_count','dislike_count']).reset_index(drop=True)
    ans1.index += 1
    return ans1

def question7():   
    database.execute("SELECT channel_name, channel_views FROM channel ORDER BY channel_views DESC;")
    result = database.fetchall()
    ans1 = pd.DataFrame(result, columns=['channel_name', 'channel_views']).reset_index(drop=True)
    ans1.index += 1
    return ans1

def question8():   
    database.execute("SELECT channel.channel_name, video.published_date FROM channel JOIN video ON channel.channel_name = video.channel_name WHERE EXTRACT(YEAR FROM video.published_date) = 2022;")
    result = database.fetchall()
    ans1 = pd.DataFrame(result, columns=['channel.channel_name', 'video.published_date']).reset_index(drop=True)
    ans1.index += 1
    return ans1

def question9():   
    database.execute("SELECT channel_name, video_name, comment_count FROM video ORDER BY comment_count DESC NULLS last;")
    result = database.fetchall()
    ans1 = pd.DataFrame(result, columns=['channel_name', 'video_name','comment_count']).reset_index(drop=True)
    ans1.index += 1
    return ans1
    
