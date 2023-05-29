
import psycopg2 as pg2
from mongo_to_sql import channel_info_df
from mongo_to_sql import playlist_to_df
from mongo_to_sql import video_to_df
from mongo_to_sql import comments_to_df

conn = pg2.connect(host='localhost',user='postgres',password='karthi123',port=5432,database='YoutubeData')
if conn:
    print("Connection Established Successfully")
    database = conn.cursor()

def channel_table_creation(): 
    channel_df = channel_info_df()
    database.execute('DROP TABLE IF EXISTS channel CASCADE')
    database.execute(''' CREATE TABLE IF NOT EXISTS channel(
                                                        channel_id  VARCHAR(255) PRIMARY KEY,
                                                        channel_name VARCHAR(255),
                                                        channel_type VARCHAR(255),
                                                        channel_views INT,
                                                        channel_description TEXT,
                                                        channel_status VARCHAR(255)
                                                        )
                    ''')
    
    # Prepare the INSERT statement with placeholders for the values
    insert_query1 = """
        INSERT INTO channel (channel_id, channel_name, channel_type, channel_views, channel_description, channel_status)
        VALUES (%s, %s, %s, %s, %s, %s)
    """
    # Iterate over each row in the DataFrame and execute the INSERT statement
    for _, row in channel_df.iterrows():
        values = tuple(row)  # Convert the row to a tuple of values
        database.execute(insert_query1, values)
    conn.commit()
        

def playlist_table_creation():
    playlist_df = playlist_to_df()
    database.execute('DROP TABLE IF EXISTS playlist CASCADE')
    database.execute('''CREATE TABLE IF NOT EXISTS playlist(
                                    channel_name VARCHAR(255),
                                    playlist_id VARCHAR(255) PRIMARY KEY,
                                    channel_id VARCHAR(255), FOREIGN KEY (channel_id) REFERENCES channel(channel_id))
                    ''')
    insert_query2 = '''INSERT INTO playlist (channel_name, playlist_id, channel_id) VALUES (%s, %s, %s)'''
    
    for _, row in playlist_df.iterrows():
        values = tuple(row)  # Convert the row to a tuple of values
        database.execute(insert_query2, values)
    conn.commit()
        
        
def video_table_creation():
    video_df = video_to_df()
    database.execute('DROP TABLE IF EXISTS video CASCADE')
    database.execute('''CREATE TABLE IF NOT EXISTS video(
                                    channel_name VARCHAR(255),
                                    video_id VARCHAR(255) PRIMARY KEY,
                                    playlist_id VARCHAR(255) ,
                                    video_name TEXT,
                                    video_description TEXT,
                                    published_date TIMESTAMP,
                                    view_count INT,
                                    like_count INT,
                                    dislike_count INT,
                                    favorite_count INT,
                                    comment_count INT,
                                    duration VARCHAR(255),
                                    thumbnail VARCHAR(255),
                                    caption_status VARCHAR(255),
                                    FOREIGN KEY (playlist_id) REFERENCES playlist(playlist_id)
                                            )''')
    insert_query3 = '''INSERT INTO video (channel_name, video_id, playlist_id, video_name, video_description, published_date, view_count, like_count, dislike_count, favorite_count, comment_count, duration, thumbnail, caption_status)
                                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) '''
    for _, row in video_df.iterrows():
        values = tuple(row)  # Convert the row to a tuple of values
        database.execute(insert_query3, values) 
    conn.commit()
         
        
        
def comment_table_creation():
    comment_df = comments_to_df()
    database.execute('DROP TABLE IF EXISTS comment CASCADE')
    database.execute('''CREATE TABLE IF NOT EXISTS comment(
                                    channel_name VARCHAR(255),
                                    comment_id VARCHAR(255) PRIMARY KEY,
                                    video_id VARCHAR(255) REFERENCES video(video_id),
                                    comment_text TEXT,
                                    comment_author VARCHAR(255),
                                    comment_published_date TIMESTAMP
                                    )''')
    insert_query4 = '''INSERT INTO comment (channel_name, comment_id, video_id, comment_text, comment_author, comment_published_date)
                                    VALUES (%s,%s,%s,%s,%s,%s)'''
    for _, row in comment_df.iterrows():
        values = tuple(row)  # Convert the row to a tuple of values
        database.execute(insert_query4, values) 
    conn.commit() 


