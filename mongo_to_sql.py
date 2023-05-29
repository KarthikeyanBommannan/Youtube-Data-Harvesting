from pymongo import MongoClient
import pandas as pd


def channel_info_df():
    conn = MongoClient("mongodb+srv://karthikeyan:karthi007@clusters.ayv02is.mongodb.net/?retryWrites=true&w=majority")
    if conn:
        print("Connection Established Successfully")
    try:
        database = conn["Youtube"]
        collection = database["Channel_Data"]
        channel_collection = []
        for name in collection.find():
            channel_info_df =  {
                                "Channel_Id": name.get('Channel_Name', {}).get('Channel_Id'),
                                "Channel_Name": name.get('Channel_Name', {}).get('Channel_Name'),
                                "Channel_Type": name.get('Channel_Name', {}).get('Channel_Type'),
                                "Channel_Views": name.get('Channel_Name', {}).get('Channel_Views'),
                                "Channel_Description": name.get('Channel_Name', {}).get('Channel_Description') or None,
                                "Channel_Status": name.get('Channel_Name', {}).get('Channel_Status')
                                }
            channel_collection.append(channel_info_df)
    except Exception as e:
        print(f"Error Occurred while retrieving the data: {str(e)}")
    conn.close()
    return  pd.DataFrame(channel_collection)



def playlist_to_df():
    conn = MongoClient("mongodb+srv://karthikeyan:karthi007@clusters.ayv02is.mongodb.net/?retryWrites=true&w=majority")
    if conn:
        print("Connection Established Successfully")
    try:
        database = conn["Youtube"]
        collection = database["Channel_Data"]
        playlist_collection = []
        for name in collection.find():
            playlist_info = {
                                "Channel_Name": name.get('Channel_Name', {}).get('Channel_Name'),
                                "Playlist_Id": name.get('Channel_Name', {}).get('Playlist_Id'),
                                "Channel_Id": name.get('Channel_Name', {}).get('Channel_Id')
                                }
            playlist_collection.append(playlist_info)
    except Exception as e:
        print(f"Error Occurred while retrieving the data: {str(e)}")
    conn.close()
    
    return pd.DataFrame(playlist_collection)


def video_to_df():
    conn = MongoClient("mongodb+srv://karthikeyan:karthi007@clusters.ayv02is.mongodb.net/?retryWrites=true&w=majority")
    if conn:
        print("Connection Established Successfully")
    try:
        database = conn["Youtube"]
        collection = database["Channel_Data"]
        documents = collection.find()
        video_collection = []
        for document in documents:
            channel_name = document['Channel_Name']['Channel_Name']
            if channel_name:
                for key, value in document.items():
                    if key.startswith('Video_Id_'):
                        video_id = value
                        video = {
                            'Channel_Name': channel_name,
                            'Video_Id': video_id.get('Video_Id'),
                            'Playlist_Id': document['Channel_Name'].get('Playlist_Id'),
                            'Video_Name': video_id.get('Video_Name'),
                            'Video_Description': video_id.get('Video_Description'),
                            'Published_Date': video_id.get('PublishedAt'),
                            'View_Count': video_id.get('View_Count'),
                            'Like_Count': video_id.get('Like_Count'),
                            'Dislike_Count': video_id.get('Dislike_Count'),
                            'Favourite_Count': video_id.get('Favorite_Count'),
                            'Comment_Count': video_id.get('Comment_Count'),
                            'Duration': video_id.get('Duration'),
                            'Thumbnail': video_id.get('Thumbnail'),
                            'Caption_Status': video_id.get('Caption_Status')
                            }
                        video_collection.append(video)

    except Exception as e:
        print(f"Error Occurred while retrieving the data: {str(e)}")

    conn.close()

    return pd.DataFrame(video_collection)

def comments_to_df():
    conn = MongoClient("mongodb+srv://karthikeyan:karthi007@clusters.ayv02is.mongodb.net/?retryWrites=true&w=majority")
    if conn:
        print("Connection Established Successfully")
    try:
        database = conn["Youtube"]
        collection = database["Channel_Data"]
        documents = collection.find()
        comment_collection = []
        for document in documents:
            channel_name = document['Channel_Name']['Channel_Name']
            for key, value in document.items():
                if key.startswith('Video_Id_'):
                    video_id = value
                    comments = video_id.get('comments', {})
                    for comment_id, comment in comments.items():
                        comment_detail = {
                            'Channel_Name': channel_name,
                            'Comment_Id': comment.get('Comment_Id'),
                            'Video_Id': video_id.get('Video_Id'),
                            'Comment_Text': comment.get('Comment_Text'),
                            'Comment_Author': comment.get('Comment_Author'),
                            'Comment_PublishedAt':pd.to_datetime(comment.get('Comment_PublishedAt'))
                        }
                        comment_collection.append(comment_detail)
    except Exception as e:
        print(f"Error Occurred while retrieving the data: {str(e)}")
    conn.close()
    return pd.DataFrame(comment_collection)
