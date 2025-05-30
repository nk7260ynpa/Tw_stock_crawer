import requests
import pandas as pd

name = "twse"
date = "2025-05-28"

url = "http://127.0.0.1:6738"
payload = {"name": name, "date": date}

response = requests.post(url, params=payload)
df = pd.DataFrame(response.json()["data"])

print(df.head(2))