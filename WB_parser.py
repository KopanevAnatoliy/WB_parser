import requests
import time
from random import random
import pandas as pd
from os.path import exists
from datetime import datetime
import logging
from sys import stdout

class WB_parser:

    def __init__(self, query: str, n_pages: int = 1):
        self.MAX_PAGE = 60
        self.query = query
        self.n_pages = n_pages
        self.date = datetime.now().date()
        self.encoding = "utf8"
        
        self.parse()
    
    
    def try_(func):
        """
        Проверяет код ответа, пишет логи, достает json.
        """
        def _wrapper(self, *args, **kwargs):
            request = requests.get(kwargs.get("url"), params=kwargs.get("params"))
            article = kwargs.get("article")
            page = kwargs.get("page")
            if article:
                postfix = f" | article {article}"
            elif page: 
                postfix = f" | page {page}"
            else:
                postfix = ""
            if request.status_code != 200:
                logging.warning(f"{func.__name__} status code {request.status_code}{postfix}")
                return
            logging.info(f"getting data {func.__name__}{postfix}")
            json_data = request.json()            
            return func(self, json_data, **kwargs)
        return _wrapper

    @try_
    def get_priority_orders(self, json_data, **kwargs):
        """
        Получает список рекламных карточек и их позиции на странице.
        out: dict {page: {position: article}}
        """
        out_of_order = [int(i.get("id")) for i in json_data.get("adverts")]
        positions = {}
        for page in json_data.get("pages"):
            positions[page["page"]-1] = {k-1: out_of_order.pop(0) for k in page["positions"] if len(out_of_order) > 0}
            if len(out_of_order) == 0:
                break
        return positions    
        
    def get_cards(self):
        """
        Собирает список карточек с n страниц, дополняет рекламными позициями.
        out: list
        """
        ordered_articles = {}

        for page in range(0, min(self.n_pages,self.MAX_PAGE)):
            json_data = self.parse_page(**self.get_page_url(page+1), page=page)            
            ordered_articles[page] = [advert.get("id") for advert in json_data.get("data",{}).get('products',[])]
            # time.sleep(random()*1 + 1)

        priority = self.get_priority_orders(url=self.get_priority_orders_url())
        for key in priority.keys():
            for k, v in priority[key].items():
                ordered_articles.get(key, []).insert(k, v)
        return [item for sublist in ordered_articles.values() for item in sublist]
    
    @try_
    def parse_page(self, json_data, **kwargs):
        return json_data
        
    @staticmethod
    def get_basket_number(value):
        """
        Выбор сервера
        """
        conditions = [143, 287, 431, 719, 1007, 1061, 1115, 1169, 1313, 1601, 1655, 1919]
        for condition in conditions:
            if value <= condition:
                return f"{conditions.index(condition) + 1:02}"
        return "13"
    
    def get_server_path(self, article):
        """
        Собирает часть url для карточки.
        """
        vol = article // 100_000
        basket = self.get_basket_number(vol)
        part = article // 1_000
        url = f"https://basket-{basket}.wb.ru/vol{vol}/part{part}/{article}/info/"
        return url
    
    def get_priority_orders_url(self):
        """
        url с рекламными карточками.
        """
        return f"https://catalog-ads.wildberries.ru/api/v5/search?keyword={self.query}"
    
    def get_page_url(self, page):
        """
        url n-ой страницы поисковой выдачи.
        """
        url = f"https://search.wb.ru/exactmatch/ru/common/v4/search?"
        params = {
            "TestGroup": "no_test",
            "TestID": "no_test",
            "appType": "1",
            "curr": "rub",
            "dest": "-1257786",
            "page": page,
            "query": self.query,
            "regions": "80,38,4,64,83,33,68,70,69,30,86,75,40,1,66,110,22,31,48,71,114",
            "resultset": "catalog",
            "sort": "popular",
            "spp": "0",
            "suppressSpellcheck": "false"}
        return {"url": url, "params": params}
    
    def get_main_data_url(self, article):
        """
        url основной информации о карточке.
        """
        path = self.get_server_path(article)
        return f"{path}ru/card.json" 
    
    def get_qnt_url(self, article):
        """
        url с количеством продаж.
        """
        return f"https://product-order-qnt.wildberries.ru/by-nm/?nm={article}"
    
    def get_sub_data_url(self, article):
        """
        url дополнительной информации о карточке.
        """
        return f"https://card.wb.ru/cards/detail?appType=1&nm={article}"
    
    def get_history_url(self, article):
        """
        url с историческими ценами карточки.
        """
        path = self.get_server_path(article)
        return f"{path}price-history.json"
    
    @try_
    def parse_qnt(self, json_data, **kwargs):
        """
        парсинг количества проданных товаров.
        """        
        file_name = "qnt.csv"
        data = (kwargs.get("article"), json_data[0].get("qnt"), self.date)
        self.save_to_csv([data], ["article", "qnt", "parse_date"], file_name)
    
    @try_    
    def parse_sub_data(self, json_data, **kwargs):
        """
        парсинг дополнительной информации о карточке.
        """  
        file_name = "sub_data.csv"
        data = [kwargs.get("article")]
        data.append( json_data.get("data", {}).get("products", [{}])[0].get("priceU") )
        data.append( json_data.get("data", {}).get("products", [{}])[0].get("salePriceU") )
        data.append( json_data.get("data", {}).get("products", [{}])[0].get("logisticsCost") )
        data.append( json_data.get("data", {}).get("products", [{}])[0].get("sale") )
        data.append( json_data.get("data", {}).get("products", [{}])[0].get("reviewRating") )
        data.append( json_data.get("data", {}).get("products", [{}])[0].get("feedbacks") )
        self.save_to_csv(
            [data], 
            ["article", "price", "salePrice", "logisticsCost", "discount", "rating", "feedbacks"],
            file_name)
    
    @try_
    def parse_history(self, json_data, **kwargs):
        """
        парсинг истории цены.
        """  
        file_name = "history.csv"
        data = [ (kwargs.get("article"), 
                  row.get("dt"), 
                  row.get("price", {}).get("RUB"), 
                  self.date)
                for row in json_data]
        self.save_to_csv(data, ["article", "timestamp", "price", "parse_date"], file_name)
                
    @try_
    def parse_main_data(self, json_data, **kwargs):
        """
        парсинг основной информации о карточке.
        """  
        file_name = "main_data.csv"        
        data = [kwargs.get("article")]
        data.append( json_data.get("imt_id") )
        data.append( json_data.get("imt_name") )
        data.append( json_data.get("subj_name") )
        data.append( json_data.get("subj_root_name") )
        data.append( json_data.get("vendor_code") )
        data.append( json_data.get("season") )
        data.append( json_data.get("description") )
        data.append( json_data.get("nm_colors_names") )
        data.append( json_data.get("contents") )
        data.append( json_data.get("selling", {}).get("brand_name") )
        data.append( json_data.get("certificate", {}).get("verified") )
        self.save_to_csv(
            [data], 
            ["article", "imt_id", "imt_name", "subj_name", 
             "subj_root_name", "vendor_code", "season", 
             "description", "nm_colors_names", "contents", 
             "brand_name", "certificate"],
            file_name)  
        
        self.parse_options(json_data)
        self.parse_compositions(json_data)
        self.parse_colors(json_data)
        self.parse_sizes(json_data)
        self.parse_kinds(json_data)
    
    def parse_options(self, json_data):
        """
        парсинг опций карточки в отдельный файл.
        """  
        file_name = "options.csv"
        data = []
        article = json_data.get("nm_id")
        for group in json_data.get("grouped_options", []):
            for option in group.get("options", []):
                data.append((article, option.get("name"), option.get("value"), self.date))
        self.save_to_csv(data, ["article", "name", "value", "parse_date"], file_name)

    def parse_compositions(self, json_data):
        """
        парсинг материалов изготовления карточки в отдельный файл.
        """  
        file_name = "compositions.csv"
        article = json_data.get("nm_id")
        data = [(article, composition.get("name"), self.date) 
                for composition in json_data.get("compositions", [])]        
        self.save_to_csv(data, ["article", "compositions", "parse_date"], file_name)
        
    def parse_colors(self, json_data):
        """
        парсинг доступных цветов в отдельный файл.
        """  
        file_name = "colors.csv"
        article = json_data.get("nm_id")
        data = [(article, color, self.date) 
                for color in json_data.get("colors", [])]        
        self.save_to_csv(data, ["article", "colors", "parse_date"], file_name)

    def parse_sizes(self, json_data):
        """
        парсинг размеров в отдельный файл.
        """  
        file_name = "sizes.csv"
        article = json_data.get("nm_id")
        data = [(article, size.get("tech_size"), self.date) 
                for size in json_data.get("sizes_table", {}).get("values", [])]        
        self.save_to_csv(data, ["article", "size", "parse_date"], file_name)

    def parse_kinds(self, json_data):
        """
        парсинг назначения товара в отдельный файл.
        """  
        file_name = "kinds.csv"
        article = json_data.get("nm_id")
        data = [(article, kind, self.date) 
                for kind in json_data.get("kinds", [])]        
        self.save_to_csv(data, ["article", "kinds", "parse_date"], file_name)
    
    def parse(self):
        """
        in: list(articles)
        парсинг карточек.
        """  
        articles = self.get_cards()
        for index, article in enumerate(articles):
            self.parse_main_data(url=self.get_main_data_url(article), article=article)
            self.parse_sub_data(url=self.get_sub_data_url(article), article=article)
            self.parse_history(url=self.get_history_url(article), article=article)
            self.parse_qnt(url=self.get_qnt_url(article), article=article)
            logging.info(f"{article} saved | index {index}")
        
    def save_to_csv(self, data, columns, file_name):
        """
        сохранения в файл.
        """
        df = pd.DataFrame(
            data, 
            columns=columns)
        df.to_csv(
            file_name,
            index=False,
            header=False if exists(file_name) else True,
            encoding=self.encoding,
            mode="a"
        )

        
if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        handlers=[
            logging.FileHandler("WB_parser.log", mode="a"),
            logging.StreamHandler(stdout)
        ], 
        format="[%(asctime)s] [%(levelname)s] [%(message)s]"
    )
    query = input("Введите запрос: ")
    n_pages = int(input("Введите количество страниц: "))
    WB_parser(query, n_pages)
        
            