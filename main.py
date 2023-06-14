from scrapy.crawler import CrawlerProcess
import requests
from more_itertools import unique_everseen
from time import sleep
from CardsSpider import CardsSpider
from os.path import exists
import converter


def get_page_url(query: str, page: int) -> str:
    """
    :param query: поисковой запрос.
    :param page: номер страницы.
    :return: url n-ой страницы поисковой выдачи.
    """
    return f"https://search.wb.ru/exactmatch/ru/common/v4/search?&dest=-1257786&page={page+1}&query={query}&resultset=catalog&sort=popular"


def get_advertising_cards_url(query: str) -> str:
    """
    :param query: поисковой запрос.
    :return: url с рекламными карточками.
    """
    return f"https://catalog-ads.wildberries.ru/api/v5/search?keyword={query}"


def get_cards(query: str, n_pages: int) -> dict:
    """
    Собирает артикулы карточек с разных страниц.
    :param query: поисковой запрос.
    :param n_pages: количество страниц.
    :return: карточки {page:[cards]}
    """
    cards = {}
    for page in range(n_pages):
        json_data = requests.get(get_page_url(query, page)).json()
        sleep(0.1)
        if len(json_data.get("data", {}).get('products', [])) == 0:
            break
        cards[page] = [advert.get("id") for advert in json_data.get("data", {}).get('products', [])]
    return cards


def get_ordered_cards(query: str, n_pages: int) -> list:
    """
    получает рекламные карточки,
    сортирует все карточки согласно поисоковой выдачи.
    :param query: поисковой запрос.
    :param n_pages: количество страниц.
    :return: упорядоченные артикулы карточек
    """
    json_data = requests.get(get_advertising_cards_url(query)).json()
    cards = get_cards(query, n_pages)
    if json_data.get("adverts"):
        advertising_cards = [int(i.get("id")) for i in json_data.get("adverts")]
    else:
        return list(unique_everseen([item for sublist in cards.values() for item in sublist]))
    
    positions = {}
    for page in json_data.get("pages"):
        positions[page.get("page")-1] = {k-1: advertising_cards.pop(0)
                                         for k in page.get("positions") if len(advertising_cards) > 0}
        if len(advertising_cards) == 0:
            break
    for key in positions.keys():
        for k, v in positions.get(key).items():
            cards.get(key, []).insert(k, v)
    return list(unique_everseen([item for sublist in cards.values() for item in sublist]))


def main():
    query = input("Введите запрос: ")
    n_pages = int(input("Введите количество страниц: "))
    articles = get_ordered_cards(query, n_pages)
    filename = "data.json"
    postfix = 1
    while True:
        if exists(file_name):
            filename = f"data_{postfix}.json"
        else:
            break

    process = CrawlerProcess(settings={
        # "DOWNLOAD_DELAY": 0.1,
        "FEEDS": {
            filename: {
                "format": "json",
                "encoding": "utf8"},
        },
    })

    process.crawl(CardsSpider, articles=articles)
    process.start()
    print(f"Количество карточек: {len(articles)}")
    converter.main(filename)


if __name__ == "__main__":
    main()

  