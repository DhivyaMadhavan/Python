
#pip install google-api-python-client
import streamlit as st
import time
from datetime import datetime
from googleapiclient.discovery import build
from pprint import pprint
import pandas as pd
import pymongo
import psycopg2

st.set_page_config(layout="wide")

###############
page_bg_img ="""
<style>
[data-testid="stAppViewContainer"]{
       background: #6fbfb2;
       background: -webkit-linear-gradient(0deg, #6fbfb2 0%, #89ec93 100%);
       background: linear-gradient(0deg, #6fbfb2 0%, #89ec93 100%);
        
}
</style>
"""
sidepage_bg_img ="""
<style>
[data-testid="stSidebar"][aria-expanded="true"]{
        background: #6fbfb2;
        ; 
</style>
"""


pd.set_option('display.max_columns', None)


with st.spinner("Loading..."):
    time.sleep(2)

st.markdown(page_bg_img,unsafe_allow_html=True)
st.markdown(sidepage_bg_img,unsafe_allow_html=True)
#st.markdown('<style>body{background-color: black;}</style>',unsafe_allow_html=True)
stSidebarContainer = st.sidebar
with st.sidebar:
   
    st.header(':blue[Youtube Data Harvesting & Warehousing]',divider = 'rainbow')
    stid = st.sidebar.text_input("Enter youtube channel Id")
    stdata = st.sidebar.button("Get Data")
    stmon = st.sidebar.button("Store data in MongoDB")
    stmigrate = st.sidebar.button("Migrate to SQL")
    st.write('## :blue[Select any question to get Insights]')
    question = st.selectbox('**select questions**',(
                               '1. Which channels have the most number of videos, and how many videos do they have?',
                               '2. What are the top 10 most viewed videos and their respective channels?',
                               '3. How many comments were made on each video, and what are their corresponding video names?',          
                               '4. What is the total number of views for each channel, and what are their corresponding channel names?'),
                               
                              key='collection_question')
        

##################

global cid,channels,playlist_id,cname, video_ids,playid,channel_df,video_df,comment_df,mycol
cid = stid
#establish connection from youtube api
def API_connect():
    API_key='AIzaSyCb8nYWv9g5O_PfKbQ8hvpQxpnemw_qJas'
    youtube = build('youtube', 'v3', developerKey=API_key)
    return youtube

youtube = API_connect()

def check_valid_id(youtube,channel_ids):
    try :
        try:
            channel_request = youtube.channels().list(
                part = 'snippet,statistics,contentDetails',
                id = channel_ids)
            
            channel_response = channel_request.execute()

            if 'items' not in channel_response:
                st.write(f"Invalid channel id: {channel_ids}")
                st.error("Enter the correct 11-digit **channel_id**")
                return None
                    
        except: 
            st.error('Server error (or) Check your internet connection (or) Please Try again after a few minutes', icon='🚨')
            st.write("An error occurred:")
            return None
            
    except:
        st.write('You have exceeded your YouTube API quota. Please try again tomorrow.')

 
def get_channel_data(youtube,channel_ids):
    global channels,playlist_id,channel_id
    #channnel_id = channelid
    request = youtube.channels().list(id=channel_ids,part = 'snippet,statistics,contentDetails,status')
    response = request.execute()
    for i in range(len(response['items'])):
      channels = dict(
                  channel_id= response['items'][i]['id'],
                  channel_name= response['items'][i]['snippet']['title'],
                  channel_description= response['items'][i]['snippet']['description'],
                  channel_published_date= response['items'][i]['snippet']['publishedAt'],
                  channel_type= response['items'][i]['kind'],
                  channel_views= response['items'][i]['statistics']['viewCount'],
                  channel_status=response['items'][i]['status']['privacyStatus'],
                  subscriber_count = response['items'][i]['statistics']['subscriberCount'],
                  video_count = response['items'][i]['statistics']['videoCount'],
                  playlist_id = response['items'][i]['contentDetails']['relatedPlaylists']['uploads']     )
    st.write("Retrieved channel data")
    return channels
    

def get_playlist_id(youtube,cid):
    playlistid = []
    request = youtube.channels().list(id=cid,part = 'contentDetails')
    response = request.execute()
    for i in range(len(response['items'])):
          playlistid.append(response['items'][i]['contentDetails']['relatedPlaylists']['uploads'] )
    st.write("Retrieved playlist id")
    return  
 
def get_playlist_data(youtube, p_id):
    request = youtube.playlists().list(part='snippet', id=p_id)
    response = request.execute()   
    
    for i in range(len(response['items'])):
        pids = dict(playlist_id=response['items'][i].get('id',0),
                    channel_id=response['items'][i]['snippet']['channelId'],
                    playlist_name=response['items'][i]['snippet']['title'])   
    st.write("Retrieved playlist data")
    return pids

def get_video_ids(youtube, playlist_id,):
    video_id = []
    next_page_token = None
    for i in playlist_id:
        request = youtube.playlistItems().list(part='contentDetails',playlistId=playlist_id, maxResults=50,pageToken=next_page_token)
        response = request.execute()
        # Get video IDs
        for item in response['items']:
            video_id.append(item['contentDetails']['videoId'])

        # Check if there are more pages
        next_page_token = response.get('nextPageToken')
        if not next_page_token:
            break
    st.write("Retrieved video ids")
    return video_id
      
    
def get_video_data(youtube,video_ids):
    global video_data,videos
    video_data = []
    # Get video details
   
    for i in video_ids:
            request = youtube.videos().list(part='snippet, statistics, contentDetails',id=i,maxResults = 50)
            response = request.execute()

            for j in range(len(response['items'])):
                videos = dict(
                    video_id = response['items'][j]['id'],
                    video_name = response['items'][j]['snippet']['title'],
                    video_description = response['items'][j]['snippet']['description'],
                    channel_id = response['items'][j]['snippet']['channelId'],
                    tags = response['items'][j]['snippet'].get('tags', []),
                    published_at = response['items'][j]['snippet']['publishedAt'],
                    view_count = response['items'][j]['statistics']['viewCount'],
                    like_count = response['items'][j]['statistics'].get('likeCount', 0),
                    dislike_count = response['items'][j]['statistics'].get('dislikeCount', 0),
                    favorite_count = response['items'][j]['statistics'].get('favoriteCount', 0),
                    comment_count = response['items'][j]['statistics'].get('commentCount', 0),
                    duration = response['items'][j].get('contentDetails', {}).get('duration', 'Not Available'),
                    thumbnail = response['items'][j]['snippet']['thumbnails']['high']['url'],
                    caption_status = response['items'][j].get('contentDetails', {}).get('caption', 'Not Available'),
                    comments = 'Unavailable'
              )
            video_data.append(videos)
    st.write("Retrieved video data")    
    return video_data

def get_comment_data(youtube,video_ids):
    global comment_data,comments,data
    comment_data = []
    for i in video_ids:
        request = youtube.commentThreads().list(part="id,snippet,replies", videoId=i, maxResults=50)
        response = request.execute()
        for k in range(len(response["items"])):
            data = dict(
                comment_id = response["items"][k]["snippet"]["topLevelComment"]["id"],
                comment_text = response["items"][k]["snippet"]["topLevelComment"]["snippet"]["textDisplay"],
                comment_author = response["items"][k]["snippet"]["topLevelComment"]["snippet"]["authorDisplayName"],
                comment_publishedAt = response["items"][k]["snippet"]["topLevelComment"]["snippet"]["publishedAt"],
                video_id = response["items"][k]['snippet']['topLevelComment']["snippet"]['videoId'])
            comment_data.append(data)
    st.write("Retrieved comment data")        
    return comment_data



if stid  and stdata:
    check_valid_id(youtube,cid)
    channel_data = get_channel_data(youtube,cid)       
    play_id = get_playlist_id(youtube,cid)
    pid = channels["playlist_id"]    
    pl_data = get_playlist_data(youtube,pid)    
    video_ids = get_video_ids(youtube,pid)   
    video_data = get_video_data(youtube,video_ids)
    comment_data = get_comment_data(youtube,video_ids)
    st.write("All the youtube data are retireved successfully from Youtube API.....")

def main_mongo(youtube,cid):
    c_stats = get_channel_data(youtube,cid)
    cname = c_stats['channel_name']
    playid1 = get_playlist_id(youtube,cid)
    pid1 = c_stats["playlist_id"]    
    p_stats = get_playlist_data(youtube,pid1)
    vids = get_video_ids(youtube,pid1)
    vdata = get_video_data(youtube,vids)
    cm = get_comment_data(youtube,vids)
    data = {                
                "Channel_Name": cname,
                'ChannelDetails':c_stats,
                'PlaylistDetails':p_stats,
                'VideoDetails':vdata,
                'CommentDetails':cm
        }
if stmon == True:    
    client = pymongo.MongoClient("mongodb+srv://dhivya:Myworldd@cluster0.yjmzisp.mongodb.net/?retryWrites=true&w=majority")
    db = client["Youtube_DB"]
    col = db["Youtube_data"]
    st.write("Database and collection created in mongoDB") 
    c_stats = get_channel_data(youtube,cid)
    cname = c_stats['channel_name']
    playid1 = get_playlist_id(youtube,cid)
    pid1 = c_stats["playlist_id"]    
    p_stats = get_playlist_data(youtube,pid1)
    vids = get_video_ids(youtube,pid1)
    vdata = get_video_data(youtube,vids)
    cm = get_comment_data(youtube,vids)
    data = {    '_id'   :stid ,       
                "Channel_Name": cname,
                'ChannelDetails':c_stats,
                'PlaylistDetails':p_stats,
                'VideoDetails':vdata,
                'CommentDetails':cm
        }
    col.insert_one(data)

    
    

if stmigrate == True:  
        with st.spinner('Data uploading...'):
            time.sleep(5)
      
        client = pymongo.MongoClient("mongodb+srv://dhivya:Myworldd@cluster0.yjmzisp.mongodb.net/?retryWrites=true&w=majority")
    
        db1 = client['Youtube_DB']
        col1 = db1["Youtube_data"]
        document_names = []
        channel_list= []
        play_list = []
        video_list = []
        comment_list = []
        playl_id = []
        
        for i in col1.find():      
            if(i['_id'] == stid):
                channel_list.append(i['ChannelDetails'])
                playl_id.append(i['ChannelDetails']['playlist_id'] )
        channel_df = pd.DataFrame(channel_list) 
        
 
        
        channel_df["subscriber_count"] = channel_df["subscriber_count"].astype(int)
        #transforming channel views to int
        channel_df["channel_views"] = channel_df["channel_views"].astype(int)
        #transforming video count to int
        channel_df["video_count"] = channel_df["video_count"].astype(int)
        #transforming published date to date time
        channel_df["channel_published_date"] = pd.to_datetime(channel_df["channel_published_date"])
        channel_df.fillna('Data unavailable',inplace=True)

        
        for i in col1.find():      
            if(i['_id'] == stid):
                play_list.append(i['PlaylistDetails'])
        play_df = pd.DataFrame(play_list)                   
        st.write(play_df)      
       

        #col3 = db["VideoDetails"]

        for i in col1.find():      
            if(i['_id'] == stid):                  
                for j in i['VideoDetails']:
                  video_list.append(j)
        video_df = pd.DataFrame(video_list)                   
         

        extracted_col = channel_df["playlist_id"]
        video_df = video_df.join(extracted_col)    
        video_df['playlist_id'] =  video_df ['playlist_id'].fillna(playl_id[0])               
        st.write(video_df)    

        video_df["view_count"] = video_df["view_count"].astype(int)
        #transforming channel views to int
        video_df["like_count"] = video_df["like_count"].astype(int)
        #transforming video count to int
        video_df["comment_count"] = video_df["comment_count"].astype(int)
        #transforming published date to date time
        video_df["published_at"] = pd.to_datetime(video_df["published_at"])
        video_df.fillna('Data unavailable',inplace = True)

        for i in col1.find():      
            if(i['_id'] == stid):
                for j in i['CommentDetails']:
                  comment_list.append(j)
        comment_df = pd.DataFrame(comment_list)                   
        st.write(comment_df)
                       
       

        #transforming published date to date time
        comment_df["comment_publishedAt"] = pd.to_datetime(comment_df["comment_publishedAt"])
        comment_df["comment_publishedAt"] = comment_df["comment_publishedAt"].dt.strftime('%Y-%m-%d %H:%M:%S')
        comment_df.fillna('Data unavailable')
        st.write("Data transformation is done")
        #extracted_col = play_df["playlist_id"] 
        #comment_df = comment_df.join(extracted_col)
        #comment_df = play_df['playlist_id'].copy()
       
        
        pd.set_option('display.max_rows', None)
        pd.set_option('display.max_columns', None)
        conn = psycopg2.connect(host = "localhost",user = "postgres",password="Disha400",port="5432",database = "youtube_data")
        sqldb=conn.cursor()
        st.write("connection established") 

        


        def sql_create_channel_data():        

            c = "CREATE TABLE if not exists Channel_details (channel_id varchar(255) PRIMARY KEY,channel_name varchar(255),channel_description varchar(2000),channel_published_Date date,channel_views int,subscriber_count int,video_count int,playlist_id varchar(255))"
            sqldb.execute(c)
            for index, row in channel_df.iterrows():
                query ="insert into Channel_details(channel_id, channel_name,channel_description,channel_published_Date,channel_views,subscriber_count, video_count, playlist_id) values(%s,%s,%s,%s,%s,%s,%s,%s)"
                sqldb.execute(query, (row.channel_id,row.channel_name,row.channel_description,row.channel_published_date,row.channel_views,row.subscriber_count, row.video_count, row.playlist_id))
            conn.commit()
            st.write("channel created") 
            st.write("data inserted")

        def sql_create_playlist_data():
           
            c = "CREATE TABLE if not exists Playlist_details (playlist_id varchar(250) PRIMARY KEY,channel_id varchar(250),playlist_name varchar(250))"
            sqldb.execute(c)
            for index, row in play_df.iterrows():
                query ="insert into Playlist_details(playlist_id, channel_id, playlist_name) values(%s,%s,%s)"
                sqldb.execute(query, (row.playlist_id,row.channel_id,row.playlist_name))
            conn.commit()
            st.write("playlist created")
        
        st.write("data inserted")
        st.write("sql_create_playlist_data()hannel data and playlist inserted into table")

        def sql_create_video_data():            
            c = "CREATE TABLE if not exists Video_details (video_id varchar(255) PRIMARY KEY,video_name varchar(255), video_description varchar(5000),published_at date,view_count int,like_count int,comment_count int,channel_id varchar(255),playlist_id varchar(255))"
            sqldb.execute(c)
            for index, row in video_df.iterrows():
                
                    query ="insert into Video_details(video_id, video_name, video_description,published_at,view_count,like_count,comment_count,channel_id) values(%s,%s,%s,%s,%s,%s,%s,%s)"
                    #query1 = "ALTER TABLE Video_details ADD UNIQUE (video_id);"
                    sqldb.execute(query, (row.video_id,row.video_name,row.video_description,row.published_at,row.view_count,row.like_count,row.comment_count,row.channel_id))
                    #sqldb.execute(query1)
                
                    
            conn.commit()
            st.write("video table and data created")


        def sql_create_comment_data():          
            c = "CREATE TABLE if not exists Comment_Details(comment_id varchar(255) PRIMARY KEY, video_id varchar(250) ,comment_text varchar(5000),comment_author varchar(255),comment_publishedAt date)"
            #d = "ALTER TABLE Comment_Details ADD CONSTRAINT fk_comment FOREIGN KEY (playlist_id) REFERENCES Playlist_Details(playlist_id) MATCH FULL;"
            #e = update comment_details set channel_id = 'UUQqmjKQBKQkRbWbSntYJX0'  where playlist_id is null
            sqldb.execute(c)
            #comment_df['playlist_id'] = play_df['playlist_id'].values
            #sqldb.execute(d)
            #comment_df['playlist_id'].unique()
            #comment_df = play_df.apply(lambda col: pd.Series(col.unique()))
            #comment_df['playlist_id'] = play_df['playlist_id'].values
           
            for index, row in comment_df.iterrows():
                query ="insert into Comment_Details(comment_id, comment_text, comment_author,comment_publishedAt,video_id) values(%s,%s,%s,%s,%s)"
                sqldb.execute(query, (row.comment_id,row.comment_text,row.comment_author,row.comment_publishedAt,row.video_id))
            
            conn.commit()
            
            st.write("comment table and data created")  

       
        
        sql_create_channel_data()
        st.success('Sucess!, Channel Table Created')        
        sql_create_playlist_data()
        st.success('Sucess!, Playlist Table Created')
        sql_create_video_data()
        st.success('Sucess!, Video Table Created')        
        sql_create_comment_data()       
        st.success('Sucess!, Comment Table Created')
       

##############################3

pd.set_option('display.max_rows', None)
pd.set_option('display.max_columns', None)
conn = psycopg2.connect(host = "localhost",user = "postgres",password="Disha400",port="5432",database = "youtube_data")
sqldb=conn.cursor()

   
if question == '1. Which channels have the most number of videos, and how many videos do they have?':

    query = "SELECT channel_Name, video_Count FROM channel_details ORDER BY video_count DESC"
    sqldb.execute(query)  
    results = sqldb.fetchall()
    conn.commit()
    df = pd.DataFrame(results, columns=["channel_names", "video_count"])
    st.write(df)
elif question == '2. What are the top 10 most viewed videos and their respective channels?':

    query = "SELECT channel_Name, video_Count FROM channel_details ORDER BY video_count DESC"
    sqldb.execute(query)  
    results = sqldb.fetchall()
    conn.commit()
    df = pd.DataFrame(results, columns=["channel_names", "video_count"])
    st.write(df)    

elif question == '3. How many comments were made on each video, and what are their corresponding video names?':

    query = "select v.video_name,v.comment_count from video_details as v"
    sqldb.execute(query)  
    results = sqldb.fetchall()
    conn.commit()
    df = pd.DataFrame(results, columns=["video_name", "comment_count"])
    st.write(df)

   


elif question == '4. What is the total number of views for each channel, and what are their corresponding channel names?':
    query = 'SELECT channel_name, channel_views FROM channel_details ORDER BY channel_views DESC;'
    sqldb.execute(query)
    results = sqldb.fetchall()
    df = pd.DataFrame(results, columns=['video_names', 'like_counts'])
    st.write(df)




     
conn.commit()