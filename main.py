import streamlit as st
from pymongo import MongoClient
from googleapiclient.discovery import build
import re

# Initialize a MongoDB client and connect to your MongoDB database
client = MongoClient("mongodb+srv://Mukesh26:F5jbqWKTUcuVwBEH@cluster0.rzqyk7m.mongodb.net/?retryWrites=true&w=majority")
db = client["youtube"]
collection = db["videos"]

# Initialize YouTube API client with your API key
YOUTUBE_API_KEY = "YOUR_YOUTUBE_API_KEY"  # Replace with your API key
youtube = build('youtube', 'v3', developerKey=YOUTUBE_API_KEY)

# Create a Streamlit app
st.title("YouTube Data Fetcher")

# Create an input field for the search query
query = st.text_input("Enter search query:")

if st.button("Search"):
    try:
        # Define the YouTube API request for video search
        video_request = youtube.search().list(
            q=query,
            type='video',
            part='id,snippet',
            maxResults=2  # Adjust the number of results as needed
        )

        # Execute the video search request and retrieve search results
        video_response = video_request.execute()

        # Initialize a list to store fetched video data
        videos = []

        # Insert the retrieved YouTube video data into MongoDB
        for item in video_response['items']:
            video_id = item['id']['videoId']

            # Additional request to get video details including contentDetails and statistics
            video_details_request = youtube.videos().list(
                part='snippet,statistics,contentDetails',
                id=video_id
            )

            video_details_response = video_details_request.execute()

            # Extract video statistics
            if 'statistics' in video_details_response['items'][0]:
                video_statistics = video_details_response['items'][0]['statistics']
            else:
                video_statistics = {}

            # Extract additional video data
            view_count = int(video_statistics.get('viewCount', 0))
            like_count = int(video_statistics.get('likeCount', 0))
            dislike_count = int(video_statistics.get('dislikeCount', 0))
            favorite_count = int(video_statistics.get('favoriteCount', 0))

            # Additional request to get video comments
            video_comments_request = youtube.commentThreads().list(
                part='snippet',
                videoId=video_id,
                maxResults=5  # Adjust the number of comments as needed
            )

            video_comments_response = video_comments_request.execute()

            # Calculate the video duration
            video_content_details = video_details_response['items'][0]['contentDetails']
            video_duration = video_content_details['duration']
            duration_match = re.match(r'PT(\d+H)?(\d+M)?(\d+S)?', video_duration)
            hours = int(duration_match.group(1)[:-1]) if duration_match.group(1) else 0
            minutes = int(duration_match.group(2)[:-1]) if duration_match.group(2) else 0
            seconds = int(duration_match.group(3)[:-1]) if duration_match.group(3) else 0

            # Calculate the total duration in seconds
            total_seconds = (hours * 3600) + (minutes * 60) + seconds

            # Format the duration as HH:MM:SS
            formatted_duration = f"{hours:02d}:{minutes:02d}:{seconds:02d}"

            # Additional request to get channel data
            channel_id = item['snippet']['channelId']
            channel_data_request = youtube.channels().list(
                part='snippet,statistics',
                id=channel_id
            )

            channel_data_response = channel_data_request.execute()
            channel_data = channel_data_response['items'][0]['snippet']
            channel_statistics = channel_data_response['items'][0]['statistics']
            channel_name = channel_data['title']
            channel_type = channel_data.get('description', '')
            channel_views = int(channel_statistics.get('viewCount', 0))
            channel_description = channel_data.get('description', '')
            channel_status = 'Active'  # You can fetch the status if needed

            # Extract playlist information
            playlist_id = item['snippet'].get('playlistId', '')
            playlist_name = item['snippet'].get('playlistTitle', '')

            video_dict = {
                'Video_Id': video_id,
                'Video_Name': item['snippet']['title'],
                'Video_Description': item['snippet']['description'],
                'PublishedAt': item['snippet']['publishedAt'],
                'View_Count': view_count,
                'Like_Count': like_count,
                'Dislike_Count': dislike_count,
                'Favorite_Count': favorite_count,
                'Comment_Count': 0,  # Placeholder for Comment_Count
                'Duration': formatted_duration,
                'Thumbnail_URL': item['snippet']['thumbnails']['default']['url'],
                'Caption_Status': 'Not Available',
                'Channel_Id': channel_id,
                'Channel_Name': channel_name,
                'Channel_Type': channel_type,
                'Channel_Views': channel_views,
                'Channel_Description': channel_description,
                'Channel_Status': channel_status,
                'Playlist_Id': playlist_id,
                'Playlist_Name': playlist_name,
            }

            videos.append(video_dict)

        st.success("Data saved to MongoDB Atlas!")

        # Close the MongoDB connection
        client.close()

        # Display the fetched video data as JSON
        if videos:
            st.subheader("Fetched Video Data:")
            st.json(videos)

    except Exception as e:
        st.error(f"An error occurred: {str(e)}")
