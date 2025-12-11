import os
from datetime import datetime
from dotenv import load_dotenv
from googleapiclient.discovery import build
from youtube_transcript_api import YouTubeTranscriptApi
import psycopg2
from psycopg2.extras import execute_values

# Load environment variables
load_dotenv()

SUPABASE_CONNECTION_STRING = os.getenv("SUPABASE_CONNECTION_STRING")
google_cloud_api_key = os.getenv("google_cloud_api_key")

# Initialize YouTube API
youtube = build("youtube", "v3", developerKey=google_cloud_api_key)

# Date range
START_DATE = datetime(2024, 11, 1)
END_DATE = datetime(2025, 10, 31)

def get_playlist_videos(playlist_id):
    """Fetch all videos from a playlist within date range"""
    videos = []
    next_page_token = None
    
    while True:
        request = youtube.playlistItems().list(
            part="snippet,contentDetails",
            playlistId=playlist_id,
            maxResults=50,
            pageToken=next_page_token
        )
        response = request.execute()
        
        for item in response['items']:
            published_at = datetime.strptime(
                item['snippet']['publishedAt'], 
                '%Y-%m-%dT%H:%M:%SZ'
            )
            
            if START_DATE <= published_at <= END_DATE:
                video_id = item['contentDetails']['videoId']
                videos.append({
                    'video_id': video_id,
                    'title': item['snippet']['title'],
                    'published_at': published_at,
                    'url': f"https://www.youtube.com/watch?v={video_id}"
                })
        
        next_page_token = response.get('nextPageToken')
        if not next_page_token:
            break
    
    return videos

def get_video_comments(video_id, max_comments=100):
    """Fetch comments for a video"""
    comments = []
    
    try:
        request = youtube.commentThreads().list(
            part="snippet",
            videoId=video_id,
            maxResults=max_comments,
            textFormat="plainText"
        )
        response = request.execute()
        
        for item in response['items']:
            comment = item['snippet']['topLevelComment']['snippet']
            comments.append({
                'author': comment['authorDisplayName'],
                'text': comment['textDisplay'],
                'published_at': comment['publishedAt'],
                'like_count': comment['likeCount']
            })
    except Exception as e:
        print(f"Error fetching comments for {video_id}: {e}")
    
    return comments

def get_video_transcript(video_id):
    """Fetch transcript for a video"""
    try:
        transcript_list = YouTubeTranscriptApi.get_transcript(
            video_id, 
            languages=['ko', 'en']
        )
        transcript = ' '.join([t['text'] for t in transcript_list])
        return transcript
    except Exception as e:
        print(f"Error fetching transcript for {video_id}: {e}")
        return None

def create_tables(conn):
    """Create database tables if they don't exist"""
    cursor = conn.cursor()
    
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS videos (
            id SERIAL PRIMARY KEY,
            video_id VARCHAR(50) UNIQUE NOT NULL,
            title TEXT,
            published_at TIMESTAMP,
            url TEXT,
            transcript TEXT,
            created_at TIMESTAMP DEFAULT NOW()
        )
    """)
    
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS comments (
            id SERIAL PRIMARY KEY,
            video_id VARCHAR(50) REFERENCES videos(video_id),
            author VARCHAR(255),
            text TEXT,
            published_at TIMESTAMP,
            like_count INTEGER,
            created_at TIMESTAMP DEFAULT NOW()
        )
    """)
    
    conn.commit()
    cursor.close()

def insert_data(conn, videos_data):
    """Insert videos and comments into database"""
    cursor = conn.cursor()
    
    for video in videos_data:
        # Insert video
        cursor.execute("""
            INSERT INTO videos (video_id, title, published_at, url, transcript)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (video_id) DO NOTHING
        """, (
            video['video_id'],
            video['title'],
            video['published_at'],
            video['url'],
            video.get('transcript')
        ))
        
        # Insert comments
        if video.get('comments'):
            comments_data = [
                (
                    video['video_id'],
                    comment['author'],
                    comment['text'],
                    comment['published_at'],
                    comment['like_count']
                )
                for comment in video['comments']
            ]
            
            execute_values(cursor, """
                INSERT INTO comments (video_id, author, text, published_at, like_count)
                VALUES %s
            """, comments_data)
    
    conn.commit()
    cursor.close()

def display_first_10_rows(conn):
    """Display first 10 rows from videos and comments"""
    cursor = conn.cursor()
    
    print("\n=== First 10 Videos ===")
    cursor.execute("SELECT video_id, title, published_at FROM videos LIMIT 10")
    for row in cursor.fetchall():
        print(f"ID: {row[0]}, Title: {row[1]}, Date: {row[2]}")
    
    print("\n=== First 10 Comments ===")
    cursor.execute("SELECT video_id, author, text, like_count FROM comments LIMIT 10")
    for row in cursor.fetchall():
        print(f"Video: {row[0]}, Author: {row[1]}, Likes: {row[3]}")
        print(f"Comment: {row[2][:100]}...")
        print("-" * 80)
    
    cursor.close()

def main():
    # Extract playlist ID from URL
    playlist_id = "PL3Eb1N33oAXhNHGe-ljKHJ5c0gjiZkqDk"
    
    print("Fetching videos from playlist...")
    videos = get_playlist_videos(playlist_id)
    print(f"Found {len(videos)} videos in date range")
    
    # Connect to database
    conn = psycopg2.connect(SUPABASE_CONNECTION_STRING)
    create_tables(conn)
    
    # Process each video
    for i, video in enumerate(videos, 1):
        print(f"\nProcessing video {i}/{len(videos)}: {video['title']}")
        
        # Get comments
        video['comments'] = get_video_comments(video['video_id'])
        print(f"  - Fetched {len(video['comments'])} comments")
        
        # Get transcript
        video['transcript'] = get_video_transcript(video['video_id'])
        if video['transcript']:
            print(f"  - Fetched transcript ({len(video['transcript'])} chars)")
        
        # Insert into database
        insert_data(conn, [video])
    
    print("\n" + "="*80)
    print("Data collection complete!")
    print("="*80)
    
    # Display first 10 rows
    display_first_10_rows(conn)
    
    conn.close()

if __name__ == "__main__":
    main()
