# importing the necessary libraries
import pandas as pd
import plotly.express as px
import streamlit as st
from streamlit_option_menu import option_menu
import mysql.connector as sql
from pymongo import MongoClient
from googleapiclient.discovery import build
from PIL import Image

ch_details = []

# SETTING PAGE CONFIGURATIONS
icon = Image.open("youtubeMain.png")
st.set_page_config(page_title="Youtube Data Harvesting and Warehousing | By MK Jose",
                   page_icon=icon,
                   layout="centered",
                   initial_sidebar_state="collapsed",
                   menu_items={'About': """# This app is created by *~MK Jose~*"""})

# CREATING OPTION MENU
with st.sidebar:
    selected = option_menu(None, ["Home", "Search and Transfer", "View", "About"],
                           icons=["house-door-fill", "search", "bar-chart", "info-circle-fill"],
                           default_index=0,
                           orientation="vertical",
                           styles={"nav-link": {"font-size": "30px", "text-align": "centre", "margin": "0px",
                                                "--hover-color": "#F55D0C"},
                                   "icon": {"font-size": "30px"},
                                   "container": {"max-width": "6000px"},
                                   "nav-link-selected": {"background-color": "#B006F0"}})

# Bridging a connection with MongoDB Atlas and Creating a new database(youtubedata)
client = MongoClient(
    "mongodb+srv://Mukesh26:F5jbqWKTUcuVwBEH@cluster0.rzqyk7m.mongodb.net/?retryWrites=true&w=majority")
db = client['youtubedata']

# CONNECTING WITH MYSQL DATABASE
mydb = sql.connect(host="localhost",
                   user="root",
                   password="Mac@2698",
                   database="Youtubedata"
                   )
mycursor = mydb.cursor(buffered=True)


# BUILDING CONNECTION WITH YOUTUBE API
api_key = "AIzaSyDP8DrFapONM3zPvUdFtq0kD17iWB6IUiM"
youtube = build('youtube', 'v3', developerKey=api_key)


# FUNCTION TO GET CHANNEL DETAILS
def get_channel_details(channel_id):
    ch_data = []
    response = youtube.channels().list(part='snippet,contentDetails,statistics',
                                       id=channel_id).execute()

    if 'items' in response and len(response['items']) > 0:
        for i in range(len(response['items'])):
            data = dict(Channel_id=channel_id[i],
                        Channel_name=response['items'][i]['snippet']['title'],
                        Playlist_id=response['items'][i]['contentDetails']['relatedPlaylists']['uploads'],
                        Subscribers=response['items'][i]['statistics']['subscriberCount'],
                        Views=response['items'][i]['statistics']['viewCount'],
                        Total_videos=response['items'][i]['statistics']['videoCount'],
                        Description=response['items'][i]['snippet']['description'],
                        Country=response['items'][i]['snippet'].get('country')
                        )
            ch_data.append(data)
    else:
        print("No items found in API response.")

    return ch_data


# FUNCTION TO GET VIDEO IDS
def get_channel_videos(channel_id, max_results):
    video_ids = []
    # get Uploads playlist id
    res = youtube.channels().list(id=channel_id, part='contentDetails').execute()

    playlist_id = res['items'][0]['contentDetails']['relatedPlaylists']['uploads']
    next_page_token = None

    while True:
        res = youtube.playlistItems().list(playlistId=playlist_id, part='snippet',
                                           maxResults=min(max_results, 50),  # Use max_results here

                                           pageToken=next_page_token).execute()

        for i in range(len(res['items'])):
            video_ids.append(res['items'][i]['snippet']['resourceId']['videoId'])
        next_page_token = res.get('nextPageToken')

        if next_page_token is None or len(video_ids) >= max_results:  # Check if we've reached max_results
            break
    return video_ids


# FUNCTION TO GET VIDEO DETAILS
def get_video_details(v_ids, max_results):
    video_stats = []

    for i in range(0, len(v_ids), max_results):
        response = youtube.videos().list(
            part="snippet,contentDetails,statistics",
            id=','.join(v_ids[i:i + max_results])).execute()

        print(response)  # Add this line for debugging

        for video in response['items']:
            video_details = dict(Channel_name=video['snippet']['channelTitle'],
                                 Channel_id=video['snippet']['channelId'],
                                 Video_id=video['id'],
                                 Title=video['snippet']['title'],
                                 Tags=video['snippet'].get('tags'),
                                 Thumbnail=video['snippet']['thumbnails']['default']['url'],
                                 Description=video['snippet']['description'],
                                 Published_date=video['snippet']['publishedAt'],
                                 Duration=video['contentDetails']['duration'],
                                 Views=video['statistics']['viewCount'],
                                 Likes=video['statistics'].get('likeCount'),
                                 Comments=video['statistics'].get('commentCount'),
                                 Favorite_count=video['statistics']['favoriteCount'],
                                 Definition=video['contentDetails']['definition'],
                                 Caption_status=video['contentDetails']['caption']
                                 )
            video_stats.append(video_details)
    return video_stats


# FUNCTION TO GET COMMENT DETAILS
def get_comments_details(v_id):
    comment_data = []
    try:
        next_page_token = None
        while True:
            response = youtube.commentThreads().list(part="snippet,replies",
                                                     videoId=v_id,
                                                     maxResults=5,
                                                     pageToken=next_page_token).execute()
            for cmt in response['items']:
                data = dict(Comment_id=cmt['id'],
                            Video_id=cmt['snippet']['videoId'],
                            Comment_text=cmt['snippet']['topLevelComment']['snippet']['textDisplay'],
                            Comment_author=cmt['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                            Comment_posted_date=cmt['snippet']['topLevelComment']['snippet']['publishedAt'],
                            Like_count=cmt['snippet']['topLevelComment']['snippet']['likeCount'],
                            Reply_count=cmt['snippet']['totalReplyCount']
                            )
                comment_data.append(data)
            next_page_token = response.get('nextPageToken')
            if next_page_token is None:
                break
    except:
        pass
    return comment_data

def get_playlist_details(channel_id, max_results):
    playlist_data = []

    # Fetch playlists for the channel
    next_page_token = None
    while True:
        playlists_response = youtube.playlists().list(
            part="snippet",
            channelId=channel_id,
            maxResults=min(max_results, 5),  # Use max_results here
            pageToken=next_page_token,
        ).execute()

        for playlist_item in playlists_response.get("items", []):
            playlist_id = playlist_item["id"]
            playlist_name = playlist_item["snippet"]["title"]

            data = {
                "Channel_id": channel_id,
                "Playlist_id": playlist_id,
                "Playlist_name": playlist_name,
            }

            playlist_data.append(data)

        next_page_token = playlists_response.get("nextPageToken")

        if not next_page_token or len(playlist_data) >= max_results:
            break

    return playlist_data

# Your list
my_list = [1, 2, 3, 4, 5]

try:
    # Attempt to access an element at index 10
    index = 10
    value = my_list[index]

    # Check if the index is within the valid range
    index = 3  # Replace this with the index you are trying to access
    if 0 <= index < list_length:
        value = your_list[index]
        print("Value at index", index, ":", value)
    else:
        print("Index is out of range.")
except IndexError:
    print("Index out of range error: The index does not exist in the list.")
except Exception as e:
    print("An error occurred:", e)

# Define the insert function for channels in MySQL
def insert_into_channels(channel_data):
    query = """INSERT INTO channels (Channel_id, Channel_name, Playlist_id, Subscribers, Views, Total_videos, Description, Country)
               VALUES (%s, %s, %s, %s, %s, %s, %s, %s)"""
    for data in channel_data:
        values = (
            data['Channel_id'],
            data['Channel_name'],
            data['Playlist_id'],
            data['Subscribers'],
            data['Views'],
            data['Total_videos'],
            data['Description'],
            data['Country']
        )
        mycursor.execute(query, values)
    mysql_db.commit()

# FUNCTION TO INSERT VIDEO DATA INTO MYSQL
def insert_into_videos(video_data):
    query = """INSERT INTO videos (Channel_id, Video_id, Title, Tags, Thumbnail, Description, Published_date, Duration, Views, Likes, Comments, Favorite_count, Definition, Caption_status)
               VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"""
    for data in video_data:
        values = (
            data['Channel_id'],
            data['Video_id'],
            data['Title'],
            ",".join(data.get('Tags', [])),  # Convert list of tags to a comma-separated string
            data['Thumbnail'],
            data['Description'],
            data['Published_date'],
            data['Duration'],
            data['Views'],
            data.get('Likes', 0),  # Handle the case where 'Likes' is not present in the data
            data.get('Comments', 0),  # Handle the case where 'Comments' is not present in the data
            data['Favorite_count'],
            data['Definition'],
            data['Caption_status']
        )
        mycursor.execute(query, values)
    mysql_db.commit()

# FUNCTION TO INSERT COMMENT DATA INTO MYSQL
def insert_into_comments(comment_data):
    query = """INSERT INTO comments (Comment_id, Video_id, Comment_text, Comment_author, Comment_posted_date, Like_count, Reply_count)
               VALUES (%s, %s, %s, %s, %s, %s, %s)"""
    for data in comment_data:
        values = (
            data['Comment_id'],
            data['Video_id'],
            data['Comment_text'],
            data['Comment_author'],
            data['Comment_posted_date'],
            data['Like_count'],
            data['Reply_count']
        )
        mycursor.execute(query, values)
    mysql_db.commit()

# FUNCTION TO INSERT PLAYLIST DATA INTO MYSQL
def insert_into_playlists(playlist_data):
    query = """INSERT INTO playlists (Channel_id, Playlist_id, Playlist_name)
               VALUES (%s, %s, %s)"""
    for data in playlist_data:
        values = (
            data['Channel_id'],
            data['Playlist_id'],
            data['Playlist_name']
        )
        mycursor.execute(query, values)
    mysql_db.commit()

# FUNCTION TO GET CHANNEL NAMES FROM MONGODB
def channel_names():
    ch_name = []
    for i in db.channel_details.find():
        ch_name.append(i['Channel_name'])
    return ch_name

    # HOME PAGE

if selected == "Home":
    # Title Image

    # Create two columns with adjusted layout and gap
    col1, col2 = st.columns([2, 1])  # Adjust the column widths as needed

    # Column 1 - Left side with updated text content and styling
    col1.markdown("## :orange[Project] : Data Science")
    col1.markdown("## :red[Tools to use] : Python, Pandas, Streamlit")
    col1.markdown(
        "## :green[About the project] : #Python code is utilized to analyze YouTube data, and once the data has been analyzed, it is visualized using the Steamlit app...#")
    col1.markdown(" Here are some Additional Key Points:")
    col1.markdown("  - Point 1: Python is used to create code for projects.")
    col1.markdown(
        "  - Point 2: The Pandas library is used for data loading, cleaning, transformation, analysis and storage in databases like SQL and MongoDB.")
    col1.markdown(
        "  - Point 3: The Streamlit library is used to create web applications to display the analyzed data information and images to interact with the user.")

    # Column 2 - Right side with an updated image
    col2.image("Youtube_logos.png", use_column_width=True)  # Replace "new_image.png" with your image filename
    col2.markdown("#### :blue[Project Image]")

# Search and Transfer PAGE
if selected == "Search and Transfer":
    tab1, tab2 = st.tabs(["$\huge Search $", "$\huge Transfer $"])

    # Search TAB
    with tab1:
        st.markdown("#    ")
        st.write("### Enter Channel ID :")
        ch_id = st.text_input("User Input").split(',')

        # Add the max_results input field
        max_results = st.number_input("Maximum Results", min_value=1, max_value=50, value=2)

        if st.button("Search Data"):
            if ch_id:
                ch_details = get_channel_details(ch_id)
                if ch_details:
                    st.write(f'#### Searched data from :green["{ch_details[0]["Channel_name"]}"] channel')
                    st.table(ch_details)
                else:
                    st.warning("No data found for the provided Channel ID(s).")
            else:
                st.warning("Please provide a valid Channel ID")

        if st.button("Upload to MongoDB"):
            with st.spinner('It takes a few minutes...'):
                if ch_id:
                    ch_details = get_channel_details(ch_id)
                    v_ids = get_channel_videos(ch_id, max_results)  # Pass max_results to the function
                    vid_details = get_video_details(v_ids, max_results)  # Pass max_results to the function
                    playlist_data = get_playlist_details(ch_id[0], max_results)  # Pass max_results to the function


                    def comments():
                        com_d = []
                        for i in v_ids:
                            com_d += get_comments_details(i)
                        return com_d


                    comm_details = comments()

                    collections1 = db.channel_details
                    collections1.insert_many(ch_details)

                    collections2 = db.video_details
                    collections2.insert_many(vid_details)

                    collections3 = db.comments_details
                    collections3.insert_many(comm_details)

                    collections4 = db.playlists
                    collections4.insert_many(playlist_data)
                    st.success("Upload to MongoDB successful !!")
                else:
                    st.warning("Please provide a valid Channel ID")

        # remove data from MySQL & MongoDB
        if st.button("Remove Data"):
            try:
                collections1 = db.channel_details
                collections1.delete_many({})
                collections2 = db.video_details
                collections2.delete_many({})
                collections3 = db.comments_details
                collections3.delete_many({})
                collections4 = db.playlist
                collections4.delete_many({})

                st.success("Data Removed Successfully!!!")
            except:
                st.error("Data already removed!!!")

    # Transfer TAB
        with tab2:
            st.markdown("#   ")
            st.markdown("### Select a channel to begin Transformation to SQL")

            ch_names = channel_names()
            user_inp = st.selectbox("Select channel", options=ch_names)

            if st.button("Submit"):
                with st.spinner('Transferring data...'):
                    try:
                        # Check if channel details already exist in MySQL
                        mycursor.execute("SELECT Channel_id FROM channels WHERE Channel_name = %s", (user_inp,))
                        existing_channel = mycursor.fetchone()

                        if existing_channel:
                            ch_details = get_channel_details(ch_id)
                            v_ids = get_channel_videos(ch_id, max_results)
                            vid_details = get_video_details(v_ids, max_results)
                            comm_details = comments()
                            playlist_data = get_playlist_details(ch_id[0], max_results)

                            transfer_to_mysql(ch_details, vid_details, comm_details, playlist_data)
                        else:
                            ch_id = ch_details[0]['Channel_id']
                            v_ids = get_channel_videos(ch_id, max_results)  # Pass max_results to the function
                            vid_details = get_video_details(v_ids, max_results)  # Pass max_results to the function
                            comm_details = comments()
                            playlist_data = get_playlist_details(ch_id, max_results)  # Pass max_results to the function

                            insert_into_channels(ch_details)
                            insert_into_videos(vid_details)
                            insert_into_comments(comm_details)
                            insert_into_playlists(playlist_data)

                            st.success("Data Transfer Successful!!!")
                    except Exception as e:
                        st.error(f"An error occurred: {e}")

# Remove Data Button - Remove Data from Both Databases
            if st.button("Removes Data"):
                try:
                    # Remove data from MongoDB
                    collections1 = db.channel_details
                    collections1.delete_many({})
                    collections2 = db.video_details
                    collections2.delete_many({})
                    collections3 = db.comments_details
                    collections3.delete_many({})
                    collections4 = db.playlists
                    collections4.delete_many({})

                    # Remove data from MySQL
                    mycursor.execute("DELETE FROM channels")
                    mycursor.execute("DELETE FROM videos")
                    mycursor.execute("DELETE FROM comments")
                    mycursor.execute("DELETE FROM playlists")
                    mydb.commit()

                    st.success("Data Removed Successfully from Both Databases!!!")
                except:
                    st.error("Data already removed or an error occurred!!!")

# VIEW PAGE
if selected == "View":

    st.write("## :orange[Select any question to get Insights]")
    questions = st.selectbox('Questions',
                             ['Click the question that you would like to query',
                              '1. What are the names of all the videos and their corresponding channels?',
                              '2. Which channels have the most number of videos, and how many videos do they have?',
                              '3. What are the top 10 most viewed videos and their respective channels?',
                              '4. How many comments were made on each video, and what are their corresponding video names?',
                              '5. Which videos have the highest number of likes, and what are their corresponding channel names?',
                              '6. What is the total number of likes and dislikes for each video, and what are their corresponding video names?',
                              '7. What is the total number of views for each channel, and what are their corresponding channel names?',
                              '8. What are the names of all the channels that have published videos in the year 2022?',
                              '9. What is the average duration of all videos in each channel, and what are their corresponding channel names?',
                              '10. Which videos have the highest number of comments, and what are their corresponding channel names?'])

    if questions == '1. What are the names of all the videos and their corresponding channels?':
        mycursor.execute(
            """SELECT title AS Video_Title, channel_name AS Channel_Name FROM videos ORDER BY channel_name""")
        df = pd.DataFrame(mycursor.fetchall(), columns=mycursor.column_names)
        st.write(df)

    elif questions == '2. Which channels have the most number of videos, and how many videos do they have?':
        mycursor.execute("""SELECT channel_name 
        AS Channel_Name, total_videos AS Total_Videos
                            FROM channels
                            ORDER BY total_videos DESC""")
        df = pd.DataFrame(mycursor.fetchall(), columns=mycursor.column_names)
        st.write(df)
        st.write("### :green[Number of videos in each channel :]")
        # st.bar_chart(df,x= mycursor.column_names[0],y= mycursor.column_names[1])
        fig = px.bar(df,
                     x=mycursor.column_names[0],
                     y=mycursor.column_names[1],
                     orientation='v',
                     color=mycursor.column_names[0]
                     )
        st.plotly_chart(fig, use_container_width=True)

    elif questions == '3. What are the top 10 most viewed videos and their respective channels?':
        mycursor.execute("""SELECT channel_name AS Channel_Name, title AS Video_Title, views AS Views 
                            FROM videos
                            ORDER BY views DESC
                            LIMIT 10""")
        df = pd.DataFrame(mycursor.fetchall(), columns=mycursor.column_names)
        st.write(df)
        st.write("### :green[Top 10 most viewed videos :]")
        fig = px.bar(df,
                     x=mycursor.column_names[2],
                     y=mycursor.column_names[1],
                     orientation='h',
                     color=mycursor.column_names[0]
                     )
        st.plotly_chart(fig, use_container_width=True)

    elif questions == '4. How many comments were made on each video, and what are their corresponding video names?':
        mycursor.execute("""SELECT a.video_id AS Video_id, a.title AS Video_Title, b.Total_Comments
                            FROM videos AS a
                            LEFT JOIN (SELECT video_id,COUNT(comment_id) AS Total_Comments
                            FROM comments GROUP BY video_id) AS b
                            ON a.video_id = b.video_id
                            ORDER BY b.Total_Comments DESC""")
        df = pd.DataFrame(mycursor.fetchall(), columns=mycursor.column_names)
        st.write(df)

    elif questions == '5. Which videos have the highest number of likes, and what are their corresponding channel names?':
        mycursor.execute("""SELECT channel_name AS Channel_Name,title AS Title,likes AS Likes_Count 
                            FROM videos
                            ORDER BY likes DESC
                            LIMIT 10""")
        df = pd.DataFrame(mycursor.fetchall(), columns=mycursor.column_names)
        st.write(df)
        st.write("### :green[Top 10 most liked videos :]")
        fig = px.bar(df,
                     x=mycursor.column_names[2],
                     y=mycursor.column_names[1],
                     orientation='h',
                     color=mycursor.column_names[0]
                     )
        st.plotly_chart(fig, use_container_width=True)

    elif questions == '6. What is the total number of likes and dislikes for each video, and what are their corresponding video names?':
        mycursor.execute("""SELECT title AS Title, likes AS Likes_Count
                            FROM videos
                            ORDER BY likes DESC""")
        df = pd.DataFrame(mycursor.fetchall(), columns=mycursor.column_names)
        st.write(df)

    elif questions == '7. What is the total number of views for each channel, and what are their corresponding channel names?':
        mycursor.execute("""SELECT channel_name AS Channel_Name, views AS Views
                            FROM channels
                            ORDER BY views DESC""")
        df = pd.DataFrame(mycursor.fetchall(), columns=mycursor.column_names)
        st.write(df)
        st.write("### :green[Channels vs Views :]")
        fig = px.bar(df,
                     x=mycursor.column_names[0],
                     y=mycursor.column_names[1],
                     orientation='v',
                     color=mycursor.column_names[0]
                     )
        st.plotly_chart(fig, use_container_width=True)

    elif questions == '8. What are the names of all the channels that have published videos in the year 2022?':
        mycursor.execute("""SELECT channel_name AS Channel_Name
                            FROM videos
                            WHERE published_date LIKE '2022%'
                            GROUP BY channel_name
                            ORDER BY channel_name""")
        df = pd.DataFrame(mycursor.fetchall(), columns=mycursor.column_names)
        st.write(df)

    elif questions == '9. What is the average duration of all videos in each channel, and what are their corresponding channel names?':
        mycursor.execute("""SELECT channel_name, 
                        SUM(duration_sec) / COUNT(*) AS average_duration
                        FROM (
                            SELECT channel_name, 
                            CASE
                                WHEN duration REGEXP '^PT[0-9]+H[0-9]+M[0-9]+S$' THEN 
                                TIME_TO_SEC(CONCAT(
                                SUBSTRING_INDEX(SUBSTRING_INDEX(duration, 'H', 1), 'T', -1), ':',
                            SUBSTRING_INDEX(SUBSTRING_INDEX(duration, 'M', 1), 'H', -1), ':',
                            SUBSTRING_INDEX(SUBSTRING_INDEX(duration, 'S', 1), 'M', -1)
                            ))
                                WHEN duration REGEXP '^PT[0-9]+M[0-9]+S$' THEN 
                                TIME_TO_SEC(CONCAT(
                                '0:', SUBSTRING_INDEX(SUBSTRING_INDEX(duration, 'M', 1), 'T', -1), ':',
                                SUBSTRING_INDEX(SUBSTRING_INDEX(duration, 'S', 1), 'M', -1)
                            ))
                                WHEN duration REGEXP '^PT[0-9]+S$' THEN 
                                TIME_TO_SEC(CONCAT(
                                '0:0:', SUBSTRING_INDEX(SUBSTRING_INDEX(duration, 'T', -1), 'S', -1)
                            ))
                            END AS duration_sec
                            FROM videos
                        ) AS a
                        GROUP BY channel_name
                        ORDER BY average_duration DESC""")
        df = pd.DataFrame(mycursor.fetchall(), columns=mycursor.column_names)
        st.write(df)
        st.write("### :green[Channels vs Average Video Duration (seconds) :]")
        fig = px.bar(df,
                     x=mycursor.column_names[0],
                     y=mycursor.column_names[1],
                     orientation='v',
                     color=mycursor.column_names[0]
                     )
        st.plotly_chart(fig, use_container_width=True)

    elif questions == '10. Which videos have the highest number of comments, and what are their corresponding channel names?':
        mycursor.execute("""SELECT channel_name AS Channel_Name, title AS Video_Title, comments AS Comments_Count 
                            FROM videos
                            ORDER BY comments DESC
                            LIMIT 10""")
        df = pd.DataFrame(mycursor.fetchall(), columns=mycursor.column_names)
        st.write(df)
        st.write("### :green[Top 10 most commented videos :]")
        fig = px.bar(df,
                     x=mycursor.column_names[2],
                     y=mycursor.column_names[1],
                     orientation='h',
                     color=mycursor.column_names[0]
                     )
        st.plotly_chart(fig, use_container_width=True)

# ABOUT PAGE
if selected == "About":
    st.image("youtubeMain.png", use_column_width=True)
    st.markdown("### :green[About the app] : This Streamlit app is designed to analyze YouTube data using Python and various libraries. It allows users to search for YouTube channels, transfer data to MySQL, and view insights about the data.")
    st.markdown("### :green[About the creator] : This app was created by MK Jose, a data science enthusiast, and Python developer.")
