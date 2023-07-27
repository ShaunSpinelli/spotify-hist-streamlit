import os
import streamlit as st
import pandas as pd
import numpy as np
import json
import dateutil
import plost
import boto3
from boto3.dynamodb.conditions import Key, Attr

from loguru import logger

# File Logging
logname = "file_1.log"
logger.add(logname)

DYNAMODB_ENDPOINT = os.environ.get("DYNAMO_ENDPOINT", "https://dynamodb.us-west-2.amazonaws.com")

## Utility Functions
@st.cache_data
def get_users_songs_table(user_id):
    dynamodb = boto3.resource("dynamodb", endpoint_url=DYNAMODB_ENDPOINT, region_name='us-west-2')
    
    all_songs = []
    year = '2022'# Hard coded for 2022 write now
    
    # Initial fetch of songs
    try:
        table = dynamodb.Table('Songs')
        response = table.query(
        KeyConditionExpression=Key('user').eq(user_id),
        # Limit=500, # hardcoded limit for dev
        FilterExpression=Attr('played_at').begins_with(year) 
    )
        all_songs.extend(response["Items"])
        
    except FileNotFoundError:
        return "User Does Not Exist"
    
    # Keep fetching until we've retrieved all the songs
    while 'LastEvaluatedKey' in response:
        fetch_count = 1
        logger.debug(f'More Results fetching {fetch_count}')
        response = table.query(
            KeyConditionExpression=Key('user').eq(user_id),
            FilterExpression=Attr('played_at').begins_with(year),
            ExclusiveStartKey=response['LastEvaluatedKey']
        )
        all_songs.extend(response["Items"])
        fetch_count += 1
        
    
    for song in all_songs:
        t = dateutil.parser.parse(song["played_at"])
        song["unix_time"] = t.timestamp()
        song["played_at"] = t.strftime("%c")

    all_songs.sort(key= lambda x: x.get("unix_time"), reverse=True)
    
    return all_songs

@st.cache_data
def pre_process(all_songs, UTC_offset):    
    
    # Need to add unix time, currently api does this
    for song in all_songs:
        t = dateutil.parser.parse(song["played_at"])
        song["unix_time"] = t.timestamp()
        song["played_at"] = t.strftime("%c")
    
    # Create df
    df = pd.DataFrame.from_dict(all_songs)

    # Add time information
    df['time'] = pd.to_datetime(df['unix_time'], unit="s") + pd.DateOffset(hours=UTC_offset)
    df['month'] = df['time'].dt.month
    df['weekday'] = df['time'].dt.dayofweek
    df['day_of_year'] = df['time'].dt.dayofyear
    df.set_index('time', inplace=True)
    df['hour_of_day'] = df.index.hour
    df['time_of_day'] = df.index.hour + (df.index.minute/60)
    df['date'] = pd.to_datetime(df['unix_time'], unit='s') + pd.DateOffset(hours=UTC_offset)
    

    df["artist"] = [s.split("-")[-1].strip() for s in df.name]
    # need to sort so pandas doesnt break 
    df.sort_values(by="time",ascending=True, inplace=True)
    
    return df


# Main App
st.title('Spotify History 2022')
id = st.text_input("Enter Id")

if id:
    st.text(f'Got Id')
    user_history = get_users_songs_table(id)
    utc_offset = 2 if id.startswith('fhmio') else 10 # hardcoded offset for LJKW
    df = pre_process(user_history, utc_offset)
    # st.table(df.head(n=5))
    st.text(f'Fetched over {len(df)} songs!')
    # user_hist_json = json.dumps(user_history, indent = 4)
    
    # st.download_button('Download History', user_hist_json, 'songs.json', 'application/json')
    
    ## Artists
    artist_n = st.slider('Number of artist', 0, 15, 5)
    artist_counts = df.artist.value_counts().head(n=artist_n).reset_index()
    st.header('Top Artists')
    plost.pie_chart(
        data=artist_counts,
        title=f'Top {artist_n} Artists',
        theta='count',
        color='artist')
    st.table(artist_counts.reset_index(drop=True))
    


    ## Songs
    song_n = st.slider('Number of Songs', 0, 10, 5)
    song_counts = df.name.value_counts().head(n=song_n).reset_index()
    st.header('Top Songs')
    plost.pie_chart(
        data=song_counts,
        title=f'Top {song_n} Songs',
        theta='count',
        color='name')
    st.table(song_counts.reset_index(drop=True))
    
    
    # plost.event_chart(
    #     data=df,
    #     x='time_of_day',
    #     y='weekday'
    # )
    
    
    # mon = df[df['weekday'] == 1]['hour_of_day'].value_counts().reset_index()
    # mon['weekday'] = 1
    # st.table(mon)
    
    # sun = df[df['weekday'] == 0]['hour_of_day'].value_counts().reset_index()
    # sun['weekday'] = 0
    # st.table(sun)
    
#     new_df = pd.concat([mon, sun])
#     st.table(new_df)
    
#     plost.time_hist(
#         data=new_df,
#         date='hour_of_day',
#         x_unit='hours',
#         y_unit='weekday',
#         color='count'
#     )
    
#     plost.xy_hist(
#     data=df,
#     x='hour_of_day',
#     y='weekday',
# )
    st.header('When you listen')
    plost.time_hist(data=df, date='date', x_unit='hours', y_unit='day', aggregate='count')

# st.text('all')

# st.table(df.head())