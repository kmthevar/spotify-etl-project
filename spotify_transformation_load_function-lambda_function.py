import json
import boto3
from datetime import datetime
from io import StringIO
import pandas as pd

def album(data):
    album_info_dict ={}
    album_info_list =[]
    for i in data['items']:
        album_info_dict[i['track']['album']['id']]={'album_name':i['track']['album']['name']
                                    ,'album_release_date':i['track']['album']['release_date']
                                    ,'album_total_tracks':i['track']['album']['total_tracks'] 
                                    ,'album_url':i['track']['album']['external_urls']['spotify']
                                    }
        album_info_list.append({'album_id':i['track']['album']['id']
                                    ,'album_name':i['track']['album']['name']
                                    ,'album_release_date':i['track']['album']['release_date']
                                    ,'album_total_tracks':i['track']['album']['total_tracks'] 
                                    ,'album_url':i['track']['album']['external_urls']['spotify']})
    return album_info_list
 
def artist(data):
    #only artist info
    artist_list =[]
    for i in data['items']:
        for key,value in i.items():
            if key == 'track':
                for artist in value['artists']:
                    artist_dict= {'artist_id':artist['id'],'artist_name':artist['name'],'artist_url':artist['href']}
                    artist_list.append(artist_dict)
            
            
    return artist_list   

def songs(data):
    song_info=[]
    for row in data['items']:
        song_id=row['track']['id']
        song_name=row['track']['name']
        song_duration = row['track']['duration_ms']
        song_url=row['track']['external_urls']['spotify']
        song_popularity=row['track']['popularity']
        album_id=row['track']['album']['id']    
        #print( 'song name : ' ,row['track']['name'])
        artist_name=[]
        for i in row['track']['artists']:
            artist_name.append(i['name'])

        song_element={'song_id':song_id,'song_name':song_name,'song_duration':song_duration,'song_url':song_url,'song_popularity':song_popularity,
                'song_popularity':song_popularity,'album_id':album_id, 'song_artists': artist_name}
        song_info.append(song_element)
    return song_info
        
def lambda_handler(event, context):
    #get the contents from the raw json file
    s3=boto3.client('s3')
    s3_resource=boto3.resource('s3')
    Bucket='spotify-etl-project-kmt'
    key='raw_data/to_process/'
    spotify_data=[]
    spotify_keys=[]
    for file in s3.list_objects(Bucket=Bucket,Prefix=key)['Contents']:
        #print(file['Key'])
        file_key=file['Key']
        if file_key.split('.')[-1]=='json':
            response= s3.get_object(Bucket=Bucket, Key=file_key)
            content=response['Body']
            jsonObject=json.loads(content.read())
            spotify_data.append(jsonObject)
            spotify_keys.append(file_key)
    print(spotify_keys)
    #print(len(spotify_data))
    for data in spotify_data:
        album_list=album(data)
        artist_data=artist(data)
        song_data=songs(data)
        
        album_df=pd.DataFrame.from_dict(album_list)
        album_df=album_df.drop_duplicates(subset=['album_id'])

        artist_df=pd.DataFrame.from_dict(artist_data)
        artist_df=artist_df.drop_duplicates(subset=['artist_id'])

        song_df=pd.DataFrame.from_dict(song_data)
        song_df=song_df.drop_duplicates(subset=['song_id'])

        album_df['album_release_date']=pd.to_datetime(album_df['album_release_date'],format='mixed')

        song_key='transformed_data/songs_data/song_transformed_'+str(datetime.now())+'.csv'
        song_buffer=StringIO()
        song_df.to_csv(song_buffer, index=False)
        song_content= song_buffer.getvalue()
        s3.put_object(Bucket=Bucket, Key=song_key, Body=song_content)

        album_key='transformed_data/album_data/album_transformed_'+str(datetime.now())+'.csv'
        album_buffer=StringIO()
        album_df.to_csv(album_buffer, index=False)
        album_content= album_buffer.getvalue()
        s3.put_object(Bucket=Bucket, Key=album_key, Body=album_content)

        artist_key='transformed_data/artist_data/artist_transformed_'+str(datetime.now())+'.csv'
        artist_buffer=StringIO()
        artist_df.to_csv(artist_buffer, index=False)
        artist_content= artist_buffer.getvalue()
        s3.put_object(Bucket=Bucket, Key=artist_key, Body=artist_content)

    
    for key in spotify_keys:
        copy_source={
            'Bucket':Bucket, 'Key':key
        }
        s3_resource.meta.client.copy(copy_source,Bucket,'raw_data/processed/'+key.split('/')[-1])
        s3_resource.Object(Bucket, key).delete()
     
  




         

    
