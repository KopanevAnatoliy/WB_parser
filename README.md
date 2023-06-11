# WB_parser
### Парсер поисковой выдачи wildberries.ru.

Для запуска необходима запустить main.py, ввести поисковой запрос и количество страниц.

#### Полученные данные:
##### main_data.csv
- index: номер в поисковой выдачи. -> 0
- article: артикул товара. -> 144122744
- imt_id: идентификатор карточки товара. -> 79169514
- imt_name: название товара. -> Пальто весна длинное
- subj_name: подкатегория товара. -> Пальто
- subj_root_name: категория товара. -> Одежда
- vendor_code: код продавца. -> CR345_черный
- season: сезон назначения. -> демисезон; зима
- description: описание товара. -> Пальто награждено золотым зна......
- nm_colors_names: цвета товара. -> белый, черный
- contents: состав товара. -> пояс - 1 шт; Пальто - 1 шт
- brand_name: название брэнда. -> Fabian
- certificate: сертификация. -> True
- price: цена товара.(умноженная на 100) -> 149900
- salePrice: цена со скидкой.(умноженная на 100) -> 90000
- logisticsCost: цена логистики?. -> 0
- discount: размер скидки(%) -> 53
- rating: рейтинг товара -> 4.6
- feedbacks: количество отзывов -> 131
- qnt: количество проданных товаров -> 900
- kinds: назначение -> женский
\
![plot](./pictures/main_data.jpg)

##### history.csv:
###### [связь 1 ко 1]
- index: номер в поисковой выдачи. -> 0
- article: артикул товара. -> 144122744
- parse_date: дата запуска парсера. -> 2023-06-09
- {date}: цена в указанной дате.(умноженная на 100) -> 149900
- ...
- {date}: цена в указанной дате.(умноженная на 100) -> 139900
\
![plot](./pictures/history.jpg)

##### compositions.csv:
###### [связь 1 ко многим]
- index: номер в поисковой выдачи. -> 0
- article: артикул товара. -> 144122744
- compositions: материал изготовления -> шерсть
- parse_date: дата запуска парсера. -> 2023-06-09
\
![plot](./pictures/compositions.jpg)

##### options.csv
###### [связь 1 ко многим]
- index: номер в поисковой выдачи. -> 0
- article: артикул товара. -> 144122744
- name: название опции -> Фактура материала
- value: значение опции -> пальтовая ткань
- parse_date: дата запуска парсера. -> 2023-06-09
\
![plot](./pictures/options.jpg)

##### sizes.csv: [связь 1 ко многим]
###### [связь 1 ко многим]
- index: номер в поисковой выдачи. -> 0
- article: артикул товара. -> 144122744
- size: размер товара как указал продавец (XL, 52, 48-50) -> 48
- parse_date: дата запуска парсера. -> 2023-06-09
\
![plot](./pictures/sizes.jpg)

