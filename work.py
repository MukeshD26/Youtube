import streamlit as st
from googleapiclient.discovery import build

class YouTubePlaylistApp:
    def __init__(self):
        self.api_key = 'AIzaSyDgbCq4C20yaffro0daWEV-hfLLxw4icTg'
        self.youtube = build('youtube', 'v3', developerKey=self.api_key)

    def fetch_playlists(self, channel_id):
        try:
            playlists = self.youtube.playlists().list(
                channelId=channel_id,
                part='snippet',
                maxResults=10
            ).execute()

            return playlists['items']

        except Exception as e:
            st.error(f"An error occurred: {str(e)}")
            return []

    def run(self):
        st.title('YouTube Playlist Details')

        channel_id = st.text_input('Enter YouTube Channel ID:')

        if st.button('Fetch Playlists'):
            playlists = self.fetch_playlists(channel_id)

            if playlists:
                st.write('**Playlists:**')
                for playlist in playlists:
                    st.write(f"**Playlist Title: {playlist['snippet']['title']}**")
                    st.write(f"Playlist ID: {playlist['id']}")
                    st.write(f"Description: {playlist['snippet']['description']}")
                    st.write('---')

if __name__ == "__main__":
    app = YouTubePlaylistApp()
    app.run()
