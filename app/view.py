from app.application import APP
from flask import render_template
from app.core import database
from flask import request
from app.core.ML import (load_model, predict_cost)


@APP.route("/")
def index():

    q = request.args.get("q")

    page = request.args.get("page")
    if page and page.isdigit():
        if int(page) > 0:
            page = int(page)
        else:
            page = 1
    else:
        page = 1

    def prev(page_id):
        if page_id - 1 <= 0:
            return 1
        return page_id - 1

    def next(page_id):
        return page_id + 1

    def last():
        return database.DB.flats.count({}) // 30 + 1

    if q:
        flats = database.search_data(q)
    else:
        flats = database.get_flats_data(page)
    return render_template("main.html", flats=flats, page=page, prev=prev, next=next, last=last)


@APP.route("/flat/<flat_id>")
def flat_detail(flat_id):
    flat = database.get_flat_data(flat_id)
    last_cost = flat["cost_square_meter"][list(flat["cost_square_meter"].keys())[-1]]
    full_cost = int(last_cost * flat["full_area"])
    average_cost = (sum([flat["cost_square_meter"][data] for data in flat["cost_square_meter"]])
                    // len(flat["cost_square_meter"].keys()))
    model = load_model()
    if model:
        predicted_cost = predict_cost(model, database.get_flat_data(flat_id))
    else:
        predicted_cost = 0
    return render_template("flat.html", flat=flat, last_cost=last_cost, full_cost=full_cost,
                           predicted_cost=predicted_cost, average_cost=average_cost)


@APP.route("/admin")
def admin():
    return render_template("admin.html")
