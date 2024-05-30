from typing_extensions import ParamSpecKwargs
import pandas as pd
from googleapiclient.discovery import build
import re

# Set up YouTube Data API access
api_key = "AIzaSyA8sFEUjjDAsWzxlJfOCko47e-TWkQMPXU"
youtube = build('youtube', 'v3', developerKey=api_key)


#Extract Channel data using channel_id copied from a specific YouTube channel
def extract_channel_data(channel_id):
    print("Extracting Channel Data")
    request=youtube.channels().list(
                    part="snippet,ContentDetails,statistics,status,topicDetails",
                    id=channel_id
    )
    response=request.execute()

    for i in response['items']:
        channel_data=dict(Channel_Name=i["snippet"]["title"],
                Channel_Id=i["id"],
                Channel_Status=i['status']['privacyStatus'],
                Views=i["statistics"]["viewCount"],
                Total_Videos=i["statistics"]["videoCount"],
                Channel_type = i['topicDetails']['topicCategories'],
                Channel_Description=i["snippet"]["description"],
                Playlist_Id=i["contentDetails"]["relatedPlaylists"]["uploads"])
    return channel_data


#Extract video_ids of the YouTube channel using the playlist_id
def extract_video_ids(channel_Id):
    print("Extracting Videos ID's")
    video_ids=[]
    response=youtube.channels().list(id=channel_Id,
                                    part='contentDetails').execute()
    Playlist_Id=response['items'][0]['contentDetails']['relatedPlaylists']['uploads']

    next_page_token=None

    while True:
        response1=youtube.playlistItems().list(
                                            part='snippet',
                                            playlistId=Playlist_Id,
                                            maxResults=50,
                                            pageToken=next_page_token).execute()
        for i in range(len(response1['items'])):
            video_ids.append(response1['items'][i]['snippet']['resourceId']['videoId'])
        next_page_token=response1.get('nextPageToken')

        if next_page_token is None:
            break
    return video_ids


#Extract details of all the videos in the YouTube channel using the video_ids
def extract_video_data(video_Ids):
    print("Extracting Videos Data")
    video_data=[]
    for video_id in video_Ids:
        request=youtube.videos().list(
            part="snippet,ContentDetails,statistics",
            id=video_id
        )
        response=request.execute()

        for item in response["items"]:
            data=dict(
                    Channel_Name=item['snippet']['channelTitle'],
                    Channel_Id=item['snippet']['channelId'],
                    Video_Id=item['id'],
                    Video_name=item['snippet']['title'],
                    Description=item['snippet'].get('description'),
                    Published_Date=item['snippet']['publishedAt'],
                    Views=item['statistics'].get('viewCount'),
                    Likes=item['statistics'].get('likeCount'),
                    Favorite_Count=item['statistics']['favoriteCount'],
                    Comments=item['statistics'].get('commentCount'),
                    Duration=convert_to_minutes(item['contentDetails']['duration']),
                    # Tags=item['snippet'].get('tags'),
                    Thumbnail=item['snippet']['thumbnails']['default']['url'],
                    # Definition=item['contentDetails']['definition'],
                    Caption_Status=item['contentDetails']['caption'],
                    )
            video_data.append(data)    
    return video_data

# Converting the Duration of the video in minutes for easy processing of the data in database
def convert_to_minutes(time_string):
    hour_match = re.match(r'PT(?P<hours>\d+)H(?P<minutes>\d+)M(?P<seconds>\d+)S', time_string)
    hour_min_match = re.match(r'PT(?P<hours>\d+)H(?P<minutes>\d+)M', time_string)
    min_sec_match = re.match(r'PT(?P<minutes>\d+)M(?P<seconds>\d+)S', time_string)
    hour_sec_match = re.match(r'PT(?P<hours>\d+)H(?P<seconds>\d+)S', time_string)
    hour_only_match = re.match(r'PT(?P<hours>\d+)H', time_string)
    minute_match = re.match(r'PT(?P<minutes>\d+)M', time_string)
    sec_match = re.match(r'PT(?P<seconds>\d+)S', time_string)

    if hour_match:
        hours = int(hour_match.group('hours')) if hour_match.group('hours') else 0
        minutes = int(hour_match.group('minutes')) if hour_match.group('minutes') else 0
        seconds = int(hour_match.group('seconds')) if hour_match.group('seconds') else 0
        return hours * 60 + minutes + seconds / 60
    elif hour_min_match:
        hours = int(hour_min_match.group('hours')) if hour_min_match.group('hours') else 0
        minutes = int(hour_min_match.group('minutes')) if hour_min_match.group('minutes') else 0
        return hours * 60 + minutes
    elif min_sec_match:
        minutes = int(min_sec_match.group('minutes')) if min_sec_match.group('minutes') else 0
        seconds = int(min_sec_match.group('seconds')) if min_sec_match.group('seconds') else 0
        return minutes + seconds / 60
    elif hour_sec_match:
        hours = int(hour_sec_match.group('hours')) if hour_sec_match.group('hours') else 0
        seconds = int(hour_sec_match.group('seconds')) if hour_sec_match.group('seconds') else 0
        return hours * 60 + seconds / 60
    elif hour_only_match:
        hours = int(hour_only_match.group('hours')) if hour_only_match.group('hours') else 0
        return hours * 60
    elif minute_match:
        minutes = int(minute_match.group('minutes')) if minute_match.group('minutes') else 0
        return minutes
    elif sec_match:
        seconds = int(sec_match.group('seconds')) if sec_match.group('seconds') else 0
        return seconds / 60
    else:
      return 0

#Extract the first 100 comments of each video in the YouTube channel using the video_ids
def extract_comments(video_ids):
    print("Extract Comments Data")
    Comment_data=[]
    try:
        for video_id in video_ids:
            request=youtube.commentThreads().list(
                part="snippet",
                videoId=video_id,
                maxResults=50
            )
            response=request.execute()

            for item in response['items']:
                data=dict(Comment_Id=item['snippet']['topLevelComment']['id'],
                        Video_Id=item['snippet']['topLevelComment']['snippet']['videoId'],
                        Comment_Text=item['snippet']['topLevelComment']['snippet']['textDisplay'],
                        Comment_Author=item['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                        Comment_Published=item['snippet']['topLevelComment']['snippet']['publishedAt'])
                
                Comment_data.append(data)
                
    except:
        pass
    return Comment_data

#Extract all the data of the YouTube channel(Channel_data, Video_data, Comment_data) by calling the functions
def extract_data(id):
    print("Beginning Data Extraction")
    channel_id = id
    channel_data = extract_channel_data(channel_id)
    video_ids = extract_video_ids(channel_id)
    video_data = []
    comment_data = []
    video_data = extract_video_data(video_ids)
    comment_data = extract_comments(video_ids)
    # video_data.append(video_details)
    # comment_data.append(video_details['Comments'])
    video_df = pd.DataFrame(video_data)
    comment_df = pd.DataFrame(comment_data)
    return(channel_data,video_df,comment_df)





