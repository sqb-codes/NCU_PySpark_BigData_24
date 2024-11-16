# Fetching YouTube Video Comments
from googleapiclient.discovery import build
import time

def fetch_comments():
    youtube = build("youtube", "v3", developerKey=API_KEY)
    while True:
        request = youtube.commentThreads().list(part="snippet", videoId=VIDEO_ID, textFormat="plainText", maxResults=10)
        response = request.execute()
        # print(response)
        print("Fetching Comments...")
        with open("tmp/comments.txt", "a") as file:
            for item in response["items"]:
                comment = item["snippet"]["topLevelComment"]["snippet"]["textDisplay"]
                file.write(comment + "\n")
        print("Saved comments")        
        time.sleep(5)

# VIDEO_URL = "https://www.youtube.com/watch?v=RdEJQDLfLPI"
API_KEY = "AIzaSyB9J-Uu-d0pffp7P-MygIIcPjTWVp-HJQg"
VIDEO_ID = "AIBPlz5GBbY"
fetch_comments()
# print(comments)