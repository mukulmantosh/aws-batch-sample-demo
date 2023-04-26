import json
import os
from datetime import datetime, timedelta
from typing import List, Any

import backoff
import boto3
import pytz
import requests

time_zone = pytz.timezone('Asia/Kolkata')
date_time_obj = datetime.now(time_zone)
DATE = (date_time_obj.today() + timedelta(days=-1)).date()  # Get previous day
API_KEY = os.getenv("API_KEY")


def get_tickers() -> List:
    with open("tickers.txt") as file:
        data = file.readlines()
        return data


def check_status(r):
    try:
        if r.status_code == 429:
            return True
        else:
            return False
    except AttributeError:
        return False


def upload_file_to_s3(filepath, final_date, filename):
    s3 = boto3.resource('s3')
    bucket = os.getenv("S3_BUCKET")
    print(f"Uploading file {filename} to S3...")
    s3.Bucket(bucket).upload_file(filepath, f"{final_date}/{filename}")


@backoff.on_predicate(
    backoff.runtime,
    predicate=lambda r: check_status(r),
    value=lambda r: 300,
    jitter=None,
)
def process_ticker() -> Any:
    tickers_list = get_tickers()
    for ticker in tickers_list:
        ticker = ticker.replace("\n", "")
        file_name = f"{str(ticker).lower()}.json"
        file_path = os.path.abspath(file_name)
        file_exists = os.path.exists(file_path)
        if file_exists is False:
            base_url = f"https://api.polygon.io/v1/open-close/{ticker}/{DATE}?adjusted=true&apiKey={API_KEY}"
            data = requests.get(base_url)
            print(f"TICKER => {ticker} | STATUS => {data.status_code}")
            if data.status_code == 429:
                return data
            else:
                with open(file_name, "w") as file_obj:
                    file_obj.write(json.dumps(data.json()))
                    file_obj.close()
                    upload_file_to_s3(file_path, DATE, file_name)


def delete_files():
    for item in os.listdir():
        if item.endswith(".json"):
            os.remove(os.path.join("", item))


if __name__ == '__main__':
    process_ticker()
    delete_files()
