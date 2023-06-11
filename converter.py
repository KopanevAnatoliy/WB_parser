import json
import pandas as pd
from datetime import date
from datetime import datetime


def load_data(path):
    with open(path, encoding="utf8") as file:
        data = file.read()
    return json.loads(data)


def convert(data):
    sub_data = []
    qnt = []
    main_data = []
    history = []
    for row in data:
        if row.get("price"):
            sub_data.append(row)
            continue
        if row.get("qnt"):
            qnt.append(row)
            continue
        if row.get("imt_name"):
            main_data.append(row)
            continue
        if row.get("history"):
            history.append(row)
            continue
    main_data = pd.DataFrame(main_data).set_index("index")
    sub_data = pd.DataFrame(sub_data).set_index("index")
    qnt = pd.DataFrame(qnt).set_index("index")
    history = pd.DataFrame(history).set_index("index")
    pd.concat([main_data.iloc[:, :-4], sub_data.iloc[:, 2:], qnt.iloc[:, 2:], main_data.iloc[:, -1]], axis=1)\
        .to_csv("main_data.csv", encoding="utf8")
    conver_history(pd.concat([sub_data["salePrice"], history], axis=1))
    convert_options(main_data[["article", "parse_date", "options"]])
    main_data[["article", "parse_date", "compositions"]]\
        .explode("compositions").dropna(axis=0).to_csv("compositions.csv", encoding="utf8")
    main_data[["article", "parse_date", "tech_size"]].explode("tech_size")\
        .dropna(axis=0).to_csv("sizes.csv", encoding="utf8")


def convert_options(data):
    exploded = data.explode("options")
    exploded["name"] = exploded["options"].str[0]
    exploded["value"] = exploded["options"].str[1]
    exploded.drop(columns="options").to_csv("options.csv", encoding="utf8")


def conver_history(data):
    data = data.rename(columns={"salePrice": date.fromisoformat(data["parse_date"][0])} )
    prices = data.iloc[:,4:].rename(columns=lambda x: datetime.fromtimestamp(int(x)).date())
    prices = pd.concat([data.iloc[:,0], prices], axis=1)
    prices = prices.reindex(sorted(prices.columns), axis=1)
    prices = prices.fillna(method="ffill", axis=1).fillna(method="bfill", axis=1)
    pd.concat([data.iloc[:,1:3], prices], axis=1).to_csv("history.csv", encoding="utf8")


def main(path):
    convert(load_data(path))


if __name__ == "__main__":
    main("data.json")
