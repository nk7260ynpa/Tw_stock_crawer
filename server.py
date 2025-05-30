from fastapi import FastAPI

import tw_crawler

app = FastAPI()

@app.post("/")
def craw_data(name, date):
    crawlertype = name+"_crawler"
    crawler = getattr(tw_crawler, crawlertype)  # Dynamically get the class or method
    df = crawler(date)
    return {"data": df.to_dict(orient="records")}