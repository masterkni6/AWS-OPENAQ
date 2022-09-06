import boto3
import pandas as pd
import numpy as np
import os
import io
import gzip
import urllib

ACCESS_KEY = os.environ['access_key']
SECRET_KEY = os.environ['secret_key']

def lambda_handler(event, context):
    client = boto3.client(
        's3',
        aws_access_key_id=ACCESS_KEY,
        aws_secret_access_key=SECRET_KEY
    )
    
    bucket = event['Records'][0]['s3']['bucket']['name']
    key = urllib.parse.unquote(event['Records'][0]['s3']['object']['key'])
    obj = client.get_object(Bucket=bucket, Key=key)
    data = obj['Body'].read()
    try:
        with gzip.GzipFile(fileobj=io.BytesIO(data), mode='rb') as fh:
            df = pd.read_json(fh, lines=True)
    except Exception as e:
        s = str(data, 'utf-8')
        data = io.StringIO(s)
        df = pd.read_json(data, lines=True)
    #clean the data
    df = df[df['coordinates'].notna()]
    df = df[df['date'].notna()]
    df = df[df['mobile'].notna()]
    df = df[df['value'].notna()]
    df = df[df['city'].notna()]
    df['date_utc'] = df.date.apply(lambda x: x.get('utc'))
    df['year'] = pd.DatetimeIndex(df['date_utc']).year
    df['month'] = pd.DatetimeIndex(df['date_utc']).month
    df['day'] = pd.DatetimeIndex(df['date_utc']).day
    df['coordinates_latitude'] = df.coordinates.apply(lambda x: x.get('latitude'))
    df['coordinates_longitude'] = df.coordinates.apply(lambda x: x.get('longitude')) 
    df = df[['date_utc',
        'year',
        'month',
        'day',
        'parameter',
        'value',
        'unit',
        'city',
        'country',
        'coordinates_latitude',
        'coordinates_longitude',
        'sourceName',
        'sourceType',
        'mobile']]
    df.drop_duplicates(inplace=True)
    df = df[df['sourceType'].isin(['government', 'research', 'others'])]
    df = df[df['unit'].isin(['µg/m³', 'ppm'])]
    df = df[df['value'] > 0]
    #save to buffer
    out_buffer = io.BytesIO()
    df.to_parquet(out_buffer, index=False)
    name = key.split('.')[0]
    #save into silver layer
    client.put_object(Bucket='team-1-skillstorm-silver', Key=f'{name}.parquet', Body=out_buffer.getvalue())
    return {
        'statusCode': 200,
    }
