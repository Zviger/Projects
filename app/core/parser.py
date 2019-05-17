"""
Functions to parse realt.com
"""
from datetime import datetime
import requests
from bs4 import BeautifulSoup
from app.core import database
from app.core import tasks
from app.core.database import (
    DB,
    clear_db,
)

BASE_URL = "https://realt.by/sale/flats/?page="

START_URL = BASE_URL + "0"

SESSION = requests.Session()

FIELD_MAPPING = {"Область": "region",
                 "Населенный пункт": "locality",
                 "Район города": "district_city",
                 "Район (области)": "district_area",
                 "Дата обновления": "update_date",
                 "Агентство": "agency",
                 "Комнат всего/разд.": "count_rooms",
                 "Этаж / этажность": "floor_floor",
                 "Тип дома": "build_type",
                 "Планировка": "layout",
                 "Высота потолков": "ceiling_height",
                 "Год постройки": "year_built",
                 "Сан/узел": "bathroom",
                 "Примечания": "notes"}


def chunks_urls(urls, chunk_size=5):
    for j in range(0, len(urls), chunk_size):
        yield urls[j:j + chunk_size]


def chunks_urls_from_db(chunk_size=5):
    collection = DB.flats
    for j in range(0, collection.count_documents({}), chunk_size):
        urls = []
        for flat in collection.find().skip(j).limit(chunk_size):
            url = flat["url"]
            urls.append(url)
        yield urls


def apply_field_mapping(field_mapping, source):
    """
    Get data from source by FIELD_MAPPING and mapping with source
    :param field_mapping: Fields to mapping.
    :param source: A dictionary whose key is a Cyrillic field name, and a value is its value.
    :return: Dictionary for saving in data base.
    """
    data = {}
    for key in field_mapping:
        value = source.get(key)
        data[field_mapping[key]] = value
    return data


def get_html(url):
    """
    Gets html by URL.
    :param url: URL of page
    :return: HTML of page
    """
    request = SESSION.get(url)
    return request.text


def get_total_pages(html):
    """
    Gets all pages for parsing.
    :param html: HTML of page with information about total count of pages.
    :return: Total count of pages.
    """
    soap = BeautifulSoup(html, "lxml")
    pages = soap.find("div", class_="uni-paging").text.strip().split(" ")[-1]
    return int(pages)


def get_all_pages_url():
    """
    Gets all URLs for parsing by the number of pages.
    :return:
    """
    total_pages = get_total_pages(get_html(START_URL))                  # because the first page contains apartments
    urls = [BASE_URL + str(page) for page in range(total_pages)[1:]]    # from other pages
    return urls


def get_flat_data(flat_page_url):
    """
    Receives information about the apartment by URL of flat page.
    :param flat_page_url: URL of flat.
    :return: Dictionary for saving in data base.
    """
    flat_page_soap = None
    for _ in range(2):
        flat_page_html = get_html(flat_page_url)

        flat_page_soap = BeautifulSoup(flat_page_html, "lxml")
        if "504" not in str(flat_page_soap.find("head")):
            break
    if not flat_page_soap:
        return None

    _id = flat_page_url.split("/")[-2]

    try:
        title = flat_page_soap.find("h1", class_="f24").contents
        if title[0] == " ":
            title = title[2].lstrip()
        else:
            title = title[0].lstrip()
    except AttributeError:
        print(flat_page_url, "ошибка title")
        title = None

    photo_urls = []
    try:
        photos = flat_page_soap.find("div", class_="photos").find_all("div", class_="photo-item")
        for photo in photos:
            p_url = photo.find("a")
            if p_url:
                photo_urls.append(p_url.get("href"))
            else:
                photo_urls.append(photo.find("img").get("src"))
    except AttributeError:
        pass
    try:
        all_table_zebra = flat_page_soap.find_all("table", class_="table-zebra")
    except AttributeError:
        all_table_zebra = None
        print(flat_page_url, "ошибка")

    try:
        map_info = flat_page_soap.find("div", class_="buildings-map").find("div").find("div").get("data-center")
        coordinates = map_info.split("\"")
        coordinate_x = float(coordinates[9])
        coordinate_y = float(coordinates[13])
    except AttributeError:
        coordinate_x = None
        coordinate_y = None

    left_data = []
    right_data = []
    for tabel in all_table_zebra:
        try:
            left_data += [data.find("td", class_="table-row-left").text for
                          data in tabel.find_all("tr", class_="table-row")]
        except AttributeError:
            left_data.append("")

        try:
            right_data += [data.find("td", class_="table-row-right").text for
                           data in tabel.find_all("tr", class_="table-row")]

        except AttributeError:
            right_data.append("")

    some_data = dict(zip(left_data, right_data))

    result_data = apply_field_mapping(FIELD_MAPPING, some_data)

    try:
        areas = some_data["Площадь общая/жилая/кухня"].strip(" м²").split(" / ")  # area
        try:
            full_area = float(areas[0])
        except ValueError:
            full_area = None
    except KeyError:
        print(flat_page_url, "Ошибка площади")
        return None

    today = datetime.now()
    today = datetime(today.year, today.month - 1, today.day, today.hour,
                     today.minute, today.second).strftime("%Y:%m:%d:%H:%M:%S")
    try:
        cost_square_meter = some_data["Ориентировочная стоимость эквивалентна"].split(" ")[4: 5]
        if "руб/кв.м." in cost_square_meter[0]:
            cost_square_meter = [data.replace("\xa0", "").strip("руб/кв.м.") for data in cost_square_meter]
            try:
                cost_square_meter = {today: int(cost_square_meter[0])}
            except ValueError:
                return None
        else:
            cost_square_meter = [data.replace("\xa0", "").strip("руб/кв.м.") for data in cost_square_meter]
            try:
                cost_square_meter = int(cost_square_meter[0])
                cost_square_meter //= full_area
                cost_square_meter = {today: cost_square_meter}
            except (ValueError, TypeError):
                return None
    except KeyError:
        print(flat_page_url, "Ошбика цены")
        return None

    telephones = some_data.get("Телефоны")
    if telephones:
        telephones = telephones.rstrip().split("+")[1:]
    address = some_data.get("Адрес")

    if address:
        address = address.strip("Информация о доме")

    exposed = True

    dop_data = {"address": address,
                "cost_square_meter": cost_square_meter,
                "full_area": full_area,
                "url": flat_page_url,
                "telephones": telephones,
                "exposed": exposed,
                "title": title,
                "photo_urls": photo_urls,
                "_id": _id,
                "coordinate_x": coordinate_x,
                "coordinate_y": coordinate_y}

    for data in dop_data:
        result_data[data] = dop_data[data]
    return result_data


def generator_flats_data(page_url):
    """
    Generates information about apartments on the page with previews of apartments.
    :param page_url: URL of page with previews of flats.
    :return: Iterator.
    """
    html = get_html(page_url)
    soap = BeautifulSoup(html, "lxml")
    flats = []
    try:
        flats = soap.find("div", class_="tx-uedb").find_all("div", class_="bd-item")
    except AttributeError:
        yield None
    for flat in flats:
        flat_page_url = flat.find("a").get("href")
        yield get_flat_data(flat_page_url)


def update_db_with_flat(flat, today, update_cost):
    """
    It updates the cost of the apartment by the URL of the apartment, if it exists in the database.
    Otherwise, adds an apartment to the database.
    :param update_cost:
    :param flat: Some HTML data of flat.
    :param today: String with datetime.
    :return: None
    """
    if not flat:
        return
    flat_page_url = flat.find("div", class_="title").find("a").get("href")
    db_flat = database.find_flat_by_url(flat_page_url)
    if db_flat:
        if update_cost:
            try:
                current_cost_square_meter = flat.find("div", class_="bd-item-left-bottom-right"). \
                    find("span", class_="price-byr").text
                if "руб/кв.м." in current_cost_square_meter:
                    current_cost_square_meter = list(current_cost_square_meter.strip("руб/кв.м.").
                                                     split(" "))
                    current_cost_square_meter = int(current_cost_square_meter[-1].replace("\xa0", ""))
                    try:
                        cost_square_meter = db_flat["cost_square_meter"]
                        cost_square_meter[today] = current_cost_square_meter
                        database.update_db_by_cost(flat_page_url, cost_square_meter)
                    except ValueError:
                        print(flat_page_url, "Нет цены")
                elif "Цена" not in current_cost_square_meter:
                    current_cost_square_meter = int(current_cost_square_meter.strip("руб,").
                                                    replace("\xa0", ""))
                    try:
                        cost_square_meter = db_flat["cost_square_meter"]
                        full_area = db_flat["full_area"]
                        current_cost_square_meter //= full_area
                        cost_square_meter[today] = current_cost_square_meter
                        database.update_db_by_cost(flat_page_url, cost_square_meter)

                    except (ValueError, TypeError):
                        print(flat_page_url, "Нет цены")
                else:
                    cost_square_meter = db_flat["cost_square_meter"]
                    cost_square_meter[today] = None
                    database.update_db_by_cost(flat_page_url, cost_square_meter)
            except KeyError:
                pass
    else:
        data = get_flat_data(flat_page_url)
        if data:
            database.insert_data_in_db(data)


def update_db_with_page(url, update_cost=True):
    """
    Updates the cost of apartments on a page with a preview of apartments.
    :param update_cost:
    :param url: URL of page with flat.
    :return: None
    """
    today = datetime.now()
    today = datetime(today.year, today.month - 1, today.day, today.hour, today.minute, today.second).strftime(
        "%Y:%m:%d:%H:%M:%S")

    flats = None
    for _ in range(2):
        try:
            page_html = get_html(url)
            page_soap = BeautifulSoup(page_html, "lxml")
            flats = page_soap.find("div", class_="tx-uedb").find_all("div", class_="bd-item")
            break

        except (KeyError, AttributeError):
            print(url, "проблемка")

    if flats:
        for flat in flats:
            update_db_with_flat(flat, today, update_cost)


def get_urls_to_remove(urls):
    urls_to_remove = []
    for url in urls:
        html = get_html(url)
        soap = BeautifulSoup(html, "lxml")
        error_msg = soap.find("div", class_="image-404")
        if error_msg:
            urls_to_remove.append(url)
    return urls_to_remove


def create_new_db():
    clear_db()

    for urls in chunks_urls(get_all_pages_url()):
        tasks.insert_page_data_in_db.delay(urls)


def update_all_db():
    for urls in chunks_urls_from_db():
        tasks.remove_sold_flats_from_db.delay(urls)
    for urls in chunks_urls(get_all_pages_url()):
        tasks.update_db_with_pages.delay(urls)


def main():
    update_all_db()


if __name__ == "__main__":
    main()
