from pymongo import MongoClient


def init_db():
    client = MongoClient()
    return client.flats_db


DB = init_db()


def clear_db():
    DB.flats.delete_many({})


def insert_data_in_db(data):
    db = init_db()
    db.flats.insert_one(data)


def update_db_by_cost(url, cost):
    db = init_db()
    db.flats.update_one({"url": url},
                        {"$set": {"cost_square_meter": cost}})


def find_flat_by_url(url):
    return DB.flats.find_one({"url": url})


def get_flats_data(n, pagesize=30):
    return DB.flats.find().skip(pagesize * (n - 1)).limit(pagesize)


def search_data(value):
    return DB.flats.aggregate([
        {"$match": {
            "$or": [
                {'region': {'$regex': value, '$options': 'i'}},
                {'locality': {'$regex': value, '$options': 'i'}}
            ]
        }}])


def get_flat_data(_id):
    return DB.flats.find_one({"_id": _id})


def get_total_count():
    return DB.flats.count_documents({})


def main():
    print(get_total_count())


if __name__ == "__main__":
    main()
