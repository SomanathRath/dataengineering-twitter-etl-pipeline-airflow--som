import boto3
import pandas as pd

def download_csv_from_s3(bucket_name, object_key, local_path):
    s3 = boto3.client('s3')
    s3.download_file(bucket_name, object_key, local_path)

def run_twitter_etl():
    # Download the CSV from S3
    download_csv_from_s3('twitter-etl-data-som56', 'Tweet_data.csv', '/home/ubuntu/Tweet_data.csv')

    # Extract and Transform
    df = pd.read_csv('/home/ubuntu/Tweet_data.csv')
    df['text_clean'] = df['text'].str.lower().str.replace('[^a-zA-Z0-9 ]', '', regex=True)
    result = df[['tweet_id', 'airline', 'airline_sentiment', 'text_clean']]
    result.to_csv('/home/ubuntu/Tweet_data_cleaned.csv', index=False)

if __name__ == "__main__":
    run_twitter_etl()
