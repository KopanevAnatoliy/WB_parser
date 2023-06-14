import json
import pandas as pd
from datetime import date
from datetime import datetime

ENCODING = "utf8"

def load_data(path):
    with open(path, encoding=ENCODING) as file:
        data = file.read()
    return json.loads(data)


def convert(data):
    sub_data = []
    qnt = []
    main_data = []
    history = []
    for row in data:
        if "price" in row.keys():
            sub_data.append(row)
            continue
        if "qnt" in row.keys():
            qnt.append(row)
            continue
        if "imt_name" in row.keys():
            main_data.append(row)
            continue
        if "history" in row.keys():
            history.append(row)
            continue

    

    main_data = pd.DataFrame(main_data).set_index("index")
    sub_data = pd.DataFrame(sub_data).set_index("index")
    qnt = pd.DataFrame(qnt).set_index("index")
    history = pd.DataFrame(history).set_index("index")

    show_parse_statistic(main_data, sub_data, qnt, history)

    indexes = list(set(main_data.index) & set(sub_data.index) & set(qnt.index))
    main_data = main_data.loc[indexes]
    sub_data = sub_data.loc[indexes]
    qnt = qnt.loc[indexes]   

    options = convert_options(main_data)
    compositions = convert_compositions(main_data)
    history = convert_history(history, sub_data)
    sizes = convert_sizes(main_data)
    main_data = convert_main_data(sub_data, main_data, qnt)

    show_convert_statistic(main_data, options, history, compositions, sizes)

    options.to_csv("options.csv", encoding=ENCODING)
    main_data.to_csv("main_data.csv", encoding=ENCODING)
    history.to_csv("history.csv", encoding=ENCODING)
    compositions.to_csv("compositions.csv", encoding=ENCODING)
    sizes.to_csv("sizes.csv", encoding=ENCODING)

def show_parse_statistic(main_data, sub_data, qnt, history):
    print(f"Собрано:")        
    print(f"    Основные данные: {main_data.shape}")
    print(f"    Дополнительные данные: {sub_data.shape}")
    print(f"    Количество продаж: {qnt.shape}")
    print(f"    Исторические цены: {history.shape}")

def show_convert_statistic(main_data, options, history, compositions, sizes):
    print(f"Конвертировано:")        
    print(f"    Основные данные: {main_data.shape[0]}")
    print(f"    Опции: всего {options.shape[0]}, уникальных артикулов {options.index.nunique()}")
    print(f"    Материалы: всего {compositions.shape[0]}, уникальных артикулов {compositions.index.nunique()}")
    print(f"    Размеры: всего {sizes.shape[0]}, уникальных артикулов{sizes.index.nunique()}")
    print(f"    Исторические цены: всего {history.shape[0]}, уникальных артикулов{history.index.nunique()}")

def convert_sizes(main_data):
    data = main_data[["article", "parse_date", "sizes"]].explode("sizes").dropna(axis=0)
    return data

def convert_compositions(main_data):
    data = main_data[["article", "parse_date", "compositions"]].explode("compositions").dropna(axis=0)
    return data

def convert_main_data(sub_data, main_data, qnt):
    sub_data["salePrice"] = sub_data["salePrice"] / 100
    sub_data["price"] = sub_data["price"] / 100
    data =  pd.concat(
        [
            sub_data, 
            main_data.drop(["article", "parse_date", "sizes", "options", "compositions"], axis=1), 
            qnt["qnt"]
        ], 
        axis=1)
    return data

def convert_options(data):    
    exploded = data[["article", "parse_date", "options"]].explode("options")
    exploded["name"] = exploded["options"].str[0]
    exploded["value"] = exploded["options"].str[1]
    return exploded.drop(columns="options")

def convert_history(history, sub_data):
    prices = history.drop(columns=["article", "parse_date", "history"]) \
            .rename(columns=lambda x: datetime.fromtimestamp(int(x)).date())
    prices = pd.concat([prices, sub_data["salePrice"]], axis=1) \
            .rename(columns={"salePrice": datetime.now().date()})
    prices = prices.reindex(sorted(prices.columns), axis=1).fillna(method="ffill", axis=1)
    prices = prices.stack().reset_index().rename(columns={"level_1": "date", 0: "price"}).set_index("index")
    prices["price"] = prices["price"] / 100
    prices = sub_data[["article", "parse_date"]].merge(prices, on="index").reset_index()
    prev = prices.sort_values("date") \
        .groupby('article', group_keys=True) \
        .apply(lambda x: x["price"].shift(1)) \
        .droplevel(0) \
        .rename("prevPrice")
    prices = pd.concat([prices, prev], axis=1).set_index("index")
    prices["priceChange"] = (prices["price"] - prices["prevPrice"]) / prices["prevPrice"]
    return prices


def main(path):
    convert(load_data(path))


if __name__ == "__main__":
    main("data.json")
