import twse_crawler

df = twse_crawler.crawler("2024-10-23")
df.to_csv("2024-10-23.csv", index=False)