import requests
import pandas as pd

import argparse

def main(opt):
    NAME = opt.name
    DATE = opt.date

    url = "http://127.0.0.1:6738"
    payload = {"name": NAME, "date": DATE}

    response = requests.post(url, params=payload)
    json_data = response.json()["data"]
    df = pd.read_json(json_data, orient="records")

    print(df.head(2))

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Test client for fetching data.")
    parser.add_argument("--name", type=str, default="twse", help="Name of the data source")
    parser.add_argument("--date", type=str, default="2025-05-28", help="Date for the data")
    
    opt = parser.parse_args()
    main(opt)

