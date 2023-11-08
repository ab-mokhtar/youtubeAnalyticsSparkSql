# -*- coding: utf-8 -*-
"""
Created on Wed Sep 13 15:25:35 2023

@author: abbes
"""
from time import sleep
import os
from googleapiclient.discovery import build
from confluent_kafka import Producer
import json
from datetime import datetime
kafka_config = {'bootstrap.servers': 'localhost:9092'}
# Set your API key here
api_key = "AIzaSyBtNsi5qwqiAywUrYYDDPWlx75wTM3INoc"
channels=[]
#push data to kafka
def send_to_kafka(topic, message):
    producer = Producer(kafka_config)
    producer.produce(topic, key=None, value=message)
    producer.flush()
def clean_and_send_data_to_kafka(data):
    cleaned_data = []  
    channels.clear()
    for video in data:
        snippet = video.get('snippet', {})
        statistics = video.get('statistics', {})
        channels.append(snippet.get('channelId'))
       
        video_data = {
            'id':video.get('id'),
            'categoryid': snippet.get('categoryId'),
            'channelid': snippet.get('channelId'),
            'viewcount': statistics.get('viewCount'),
            'likecount': statistics.get('likeCount'),
            'dislikecount': statistics.get('dislikeCount'),
            'commentcount': statistics.get('commentCount'),
            'date':str(datetime.now()),
            'time':str(datetime.now())
        }
        cleaned_data.append(video_data)
    sleep(15)
    print(cleaned_data)
    # Send the cleaned data to Kafka
    for video_data in cleaned_data:
        
        send_to_kafka('youtubeData', json.dumps(video_data))

def clean_and_send_data_to_kafka_Channels(data):
    cleaned_channels = []  
    channels.clear()
    for channel in data:
        snippet = channel.get('snippet', {})
        statistics = channel.get('statistics', {})
      
        channel_data = {
            'id':channel.get('id'),
            'contentdetails': snippet.get('contentDetails'),
            'title': snippet.get('title'),
            'description': snippet.get('description'),
            'publishedat': snippet.get('publishedAt'),

            'viewcount': statistics.get('viewCount'),
            'videocount': statistics.get('videoCount'),
            'subscribercount': statistics.get('subscriberCount'),
            'commentcount': statistics.get('commentCount'),
            'date':str(datetime.now())
        }
        
        cleaned_channels.append(channel_data)
    print(cleaned_channels)
    # Send the cleaned data to Kafka
    for channel in cleaned_channels:
        send_to_kafka('channelData', json.dumps(channel))

def getDataForVideos():
    youtube = build('youtube', 'v3', developerKey=api_key)
    
    request = youtube.videos().list(
        part='snippet,statistics',
        chart='mostPopular',
        regionCode='MA',
        maxResults=1000
    )
    
    try:
        response = request.execute()
        video_items = response.get('items', [])
        return video_items
    except Exception as e:
        print(f"Error fetching or sending data: {str(e)}")


def getDataForChannel(channel_id):
    youtube = build('youtube', 'v3', developerKey=api_key)

    request = youtube.channels().list(
        part='snippet,statistics',
        id=channel_id 
    )

    try:
        return request.execute().get('items')[0]
        
       
    except Exception as e:
        print(f"Error fetching or sending data: {str(e)}")

def getAllChannels():
    data=[]
    for channel in channels:
        data.append(getDataForChannel(channel))
    clean_and_send_data_to_kafka_Channels(data)
if __name__ == "__main__":
    clean_and_send_data_to_kafka(getDataForVideos())
    getAllChannels()
def collecting():
    clean_and_send_data_to_kafka(getDataForVideos())
    getAllChannels()