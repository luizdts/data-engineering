import requests
import pandas as pd

datafile = "/tmp/dataset.csv"
api_key = "75FtNC8upcXjrjy7pIuUadI3U8sOr3zV"
start_date = "2024-04-01"
end_date = "2024-04-07"
url = 'https://api.polygon.io/v2/aggs/ticker/AAPL/range/1/day/{}/{}?apiKey={}'.format(start_date,end_date,api_key)
r = requests.get(url)
data = r.json()

if data["status"]=="OK":  
    #transformando o resultado em um Dataframe
    df = pd.DataFrame(data["results"])
    df["date"] = start_date

    #exportando os dados para o disco.
    df.to_csv(datafile,index=False)