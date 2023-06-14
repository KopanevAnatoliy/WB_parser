import scrapy
from datetime import datetime


class CardsSpider(scrapy.Spider):
    name = "CardsSpider"

    def start_requests(self):
        date = datetime.now().date()
        for index, article in enumerate(self.articles):
            yield scrapy.Request(
                self.get_main_data_url(article), 
                self.parse_main_data, 
                cb_kwargs={"data": {"article": article, "index": index, "parse_date": date}})
            yield scrapy.Request(
                self.get_sub_data_url(article), 
                self.parse_sub_data, 
                cb_kwargs={"data": {"article": article, "index": index, "parse_date": date}})
            yield scrapy.Request(
                self.get_history_url(article), 
                self.parse_history, 
                cb_kwargs={"data": {"article": article, "index": index, "parse_date": date}})
            yield scrapy.Request(
                self.get_qnt_url(article), 
                self.parse_qnt, 
                cb_kwargs={"data": {"article": article, "index": index, "parse_date": date}})
    
    def parse_main_data(self, response, data):
        """
        парсинг основных данных.
        """
        json_data = response.json()
        data["imt_id"] = json_data.get("imt_id")
        data["imt_name"] = json_data.get("imt_name")
        data["subj_name"] = json_data.get("subj_name")
        data["subj_root_name"] = json_data.get("subj_root_name")
        data["vendor_code"] = json_data.get("vendor_code")
        data["season"] = json_data.get("season")
        data["description"] = json_data.get("description")
        data["nm_colors_names"] = json_data.get("nm_colors_names")
        data["contents"] = json_data.get("contents")
        data["brand_name"] = json_data.get("selling", {}).get("brand_name")
        data["certificate"] = json_data.get("certificate", {}).get("verified")
        data["options"] = self.parse_options(json_data)
        data["compositions"] = [composition.get("name") for composition in json_data.get("compositions", [])]
        data["sizes"] = [size.get("tech_size") for size in json_data.get("sizes_table", {}).get("values", [])]
        data["kinds"] = json_data.get("kinds", [None])[0]
        # data["colors"] = [color for color in json_data.get("colors", [])]
        yield data

    def parse_sub_data(self, response, data):
        """
        парсинг дополнительной информации о карточке.
        """
        json_data = response.json()
        data["price"] = json_data.get("data", {}).get("products", [{}])[0].get("priceU")
        data["salePrice"] = json_data.get("data", {}).get("products", [{}])[0].get("salePriceU")
        data["logisticsCost"] = json_data.get("data", {}).get("products", [{}])[0].get("logisticsCost")
        data["sale"] = json_data.get("data", {}).get("products", [{}])[0].get("sale")
        data["rating"] = json_data.get("data", {}).get("products", [{}])[0].get("reviewRating")
        data["feedbacks"] = json_data.get("data", {}).get("products", [{}])[0].get("feedbacks")
        yield data

    def parse_history(self, response, data):
        """
        парсинг истории цены.
        """          
        json_data = response.json()
        data["history"] = True
        print({str(row.get("dt")): row.get("price", {}).get("RUB") for row in json_data})
        data |= {str(row.get("dt")): row.get("price", {}).get("RUB") for row in json_data}
        yield data

    def parse_qnt(self, response, data):
        """
        парсинг количества проданных товаров.
        """
        json_data = response.json()
        data["qnt"] = json_data[0].get("qnt")  
        yield data
    
    def parse_options(self, json_data):
        """
        парсинг опций карточки в отдельный файл.
        """  
        data = []
        for group in json_data.get("grouped_options", []):
            for option in group.get("options", []):
                data.append((option.get("name"), option.get("value")))
        return data

    def get_server_number(self, value):
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
        basket = self.get_server_number(vol)
        part = article // 1_000
        url = f"https://basket-{basket}.wb.ru/vol{vol}/part{part}/{article}/info/"
        return url

    def get_main_data_url(self, article):
        """
        url основной информации о карточке.
        """
        path = self.get_server_path(article)
        return f"{path}ru/card.json"   

    def get_sub_data_url(self, article):
        """
        url дополнительной информации о карточке.
        """
        return f"https://card.wb.ru/cards/detail?appType=1&curr=rub&dest=-1257786&spp=0&nm={article}"
    
    def get_history_url(self, article):
        """
        url с историческими ценами карточки.
        """
        path = self.get_server_path(article)
        return f"{path}price-history.json"  

    def get_qnt_url(self, article):
        """
        url с количеством продаж.
        """
        return f"https://product-order-qnt.wildberries.ru/by-nm/?nm={article}"

  