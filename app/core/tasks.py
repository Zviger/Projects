from app.core.celery_config import cel
from app.core import parser
from app.core.database import (insert_data_in_db, init_db)
from pymongo import helpers


@cel.task
def remove_sold_flats_from_db(urls):
    db = init_db()
    urls = parser.get_urls_to_remove(urls)
    db.flats.delete_many({"url": {"$in": urls}})


@cel.task
def update_db_with_pages(pages_url):
    for page_url in pages_url:
        parser.update_db_with_page(page_url, update_cost=True)


@cel.task
def insert_page_data_in_db(chunk_urls):
    for url in chunk_urls:
        for data in parser.generator_flats_data(url):
            if data:
                try:
                    insert_data_in_db(data)
                except helpers.DuplicateKeyError:
                    pass


if __name__ == "__main__":
    pass
