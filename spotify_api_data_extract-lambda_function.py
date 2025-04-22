import json
import os
import spotipy
from spotipy.oauth2 import SpotifyClientCredentials
import boto3
from datetime import datetime
def lambda_handler(event, context):
    #connect to spotify API
    client_id=os.environ.get('client_id')
    client_secret=os.environ.get('client_secret')
    client_credentials_manager=SpotifyClientCredentials(client_id=client_id,client_secret=client_secret)
    sp=spotipy.Spotify(client_credentials_manager=client_credentials_manager)

    #get playlist details from the URL
    playlist_link='https://open.spotify.com/playlist/1d2fAul5T010POHxi1bQ4i'
    playlist_URI=playlist_link.split('/')[-1]
    playlist_info=sp.playlist_tracks(playlist_URI)
    #print(playlist_info)

    filename= 'spotify_raw_'+str(datetime.now())+'.json'
    #connect to S3
    client = boto3.client('s3')
    client.put_object(Body=json.dumps(playlist_info), Bucket='spotify-etl-project-kmt', Key='raw_data/to_process/'+filename)


