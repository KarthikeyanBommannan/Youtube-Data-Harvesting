from googleapiclient.discovery import build

# import json
def get_channel_details(API_Key,channel_ids):
    all_data=[]
    api_service_name = "youtube" 
    api_version = "v3"
    youtube = build(api_service_name, api_version,developerKey=API_Key)
    
        
    # for channel_id in channel_ids:
    request = youtube.channels().list(part="snippet,contentDetails,statistics,status,topicDetails",id = channel_ids)
    response = request.execute()

    data = dict(


                                Channel_Name = response["items"][0]["snippet"]["title"],
                                Channel_Id = response["items"][0]["id"],
                                Subcription_Count = response["items"][0]["statistics"].get("subscriberCount"),
                                Channel_Views =  response["items"][0]["statistics"]["viewCount"],
                                Channel_Description = response["items"][0]["snippet"]["description"],
                                Channel_Status =  response["items"][0]["status"]["privacyStatus"],
                                Playlist_Id = response["items"][0]["contentDetails"]["relatedPlaylists"]["uploads"],
                                Channel_Type = response['items'][0]['topicDetails']['topicCategories'][0].split('/')[-1]
                           ) 
                

    all_data = {"Channel_Name":data}
      
    return all_data

def get_video_ids(API_Key,playlist_id):
    
    video_ids=[]
    
    api_service_name = "youtube" 
    api_version = "v3"
    youtube = build(api_service_name, api_version,developerKey=API_Key)
    request = youtube.playlistItems().list(
                                           part="snippet,contentDetails",
                                           playlistId = playlist_id,
                                           maxResults = 50
                                          )
    response = request.execute()

    for i in range(len(response["items"])):
        video_ids.append(response["items"][i]["contentDetails"]["videoId"])
        
    next_page_token = response.get("nextPageToken")
    next_page = True
    while next_page:
        if next_page_token is None:
            next_page = False
        else:
            request = youtube.playlistItems().list(
                                                    part="contentDetails",
                                                    maxResults = 50,
                                                    pageToken = next_page_token,
                                                    playlistId =playlist_id
                                                   )
            response = request.execute()
            for i in response["items"]:
                video_ids.append(i["contentDetails"]["videoId"])
            next_page_token = response.get("nextPageToken")
    
        
    return video_ids

def get_video_details_information(API_Key,video_ids):
    all_video_info = {}
    api_service_name = "youtube" 
    api_version = "v3"
    youtube = build(api_service_name, api_version, developerKey=API_Key)
    video_count = 0
    for i in range(0, len(video_ids), 50):
        request = youtube.videos().list(
                                        part="snippet,contentDetails,statistics",
                                        id=",".join(video_ids[i:i+50])
                                       )
        response = request.execute()

        for video in response.get("items", []):
            video_id = video.get("id")
            snippet = video.get("snippet", {})
            statistics = video.get("statistics", {})
            content_details = video.get("contentDetails", {})
            video_count+=1
            video_info = {
                
                
                    "Video_Id": video_id,
                    "Video_Name": snippet.get("title"),
                    "Video_Description": snippet.get("description"),
                    "Tags": snippet.get("tags", []),
                    "PublishedAt": snippet.get("publishedAt"),
                    "View_Count": statistics.get("viewCount"),
                    "Like_Count": statistics.get("likeCount"),
                    "Dislike_Count": statistics.get("dislikeCount"),
                    "Favorite_Count": statistics.get("favoriteCount"),
                    "Comment_Count": statistics.get("commentCount"),
                    "Duration": content_details.get("duration"),
                    "Thumbnail": snippet.get("thumbnails", {}).get("default", {}).get("url"),
                    "Caption_Status": content_details.get("caption")
                
            }
            all_video_info[f"Video_Id_{video_count}"] = video_info

    return all_video_info

def get_video_comments(API_Key,video_ids):
    all_comments = {}
    api_service_name = "youtube"
    api_version = "v3"
    
    youtube = build(api_service_name, api_version, developerKey=API_Key)

    for video in video_ids:
        try:
            next_page_token = None
            comments_in_video = {}
            comment_counter = 1

            while True:
                response = youtube.commentThreads().list(
                    part="snippet,replies",
                    videoId=video,
                    pageToken=next_page_token
                ).execute()

                for comment in response.get("items", []):
                    snippet = comment["snippet"]
                    topLevelComment = snippet["topLevelComment"]
                    comment_id = f"Comment_Id_{comment_counter}"
                    comment_info = {
                        "Comment_Id": topLevelComment["id"],
                        "Comment_Text": topLevelComment["snippet"]["textOriginal"],
                        "Comment_Author": topLevelComment["snippet"]["authorDisplayName"],
                        "Comment_PublishedAt": topLevelComment["snippet"]["publishedAt"]
                    }
                    comments_in_video[comment_id] = comment_info
                    comment_counter += 1

                next_page_token = response.get('nextPageToken')
                if not next_page_token:
                    break

            all_comments[video] = comments_in_video

        except:
            print(f"Could not find comments for video {video}")

    return all_comments

def generate_output(API_Key,channel_ids):
    channel_details = get_channel_details(API_Key,channel_ids)
    playlist_id = channel_details['Channel_Name']['Playlist_Id']
    video_ids = get_video_ids(API_Key,playlist_id)
    video_info = get_video_details_information(API_Key,video_ids)
    comments = get_video_comments(API_Key,video_ids)
    
    merged_details = {}
    count = 1
    for video_id, info, comment in zip(video_ids, video_info.values(), comments.values()):
        merged_details[f"Video_Id_{count}"] = {**info, "comments":comment}
        count+=1
    output = {**channel_details, **merged_details}
    
    return output









