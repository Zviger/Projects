from app.core.database import DB
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import StandardScaler
import sklearn.linear_model
from app.core.population_manager import DATA_SET
from joblib import dump, load

model_FILENAME = "/home/zviger/PycharmProjects/parser_realt.by/app/core/mlp.joblib"  # model linear regression model
SCALER_FILENAME = "/home/zviger/PycharmProjects/parser_realt.by/app/core/scaler.joblib"


def predict_cost(model, flat, test_mode=False):

    x, y = get_xy(flat)
    if x:
        scaler = load(SCALER_FILENAME)
        data = scaler.transform([x])
        if test_mode:
            return int(model.predict(data)), y
        else:
            return int(model.predict(data))
    else:
        if test_mode:
            return 0, y
        else:
            return 0


def get_xy(flat):
    locality = flat["locality"].split(" ")
    region = flat["region"]
    if region:
        region = DATA_SET[region]
    else:
        region = {}
        for r in DATA_SET:
            region.update(DATA_SET[r])
    if "д." in locality[0] or "г." in locality[0] or "гп." in locality[0] or \
            "кп." in locality[0] or "с/т." in locality[0] or "п." in locality[0] \
            or "х." in locality[0] or "а.г" in locality[0]:
        locality = " ".join(locality[1:])
    else:
        locality = " ".join(locality)
    x = [flat["full_area"], region[locality]]
    try:
        x.append(int(flat["year_built"]))
    except TypeError:
        x.append(1990)

    try:
        x.append(int(flat["floor_floor"][0]))
    except TypeError:
        x.append(1)

    rooms = flat["count_rooms"]
    if "доли" in rooms:
        rooms = rooms.split(" ")
        a, b = map(int, rooms[0].split("/"))
        x.append(a/b * 1000 * int(rooms[3][0]))
    elif "/" in rooms:
        x.append(int(rooms.split("/")[0]) * 1000)
    elif "комната " in rooms:
        x.append(700)
    elif "Фактически" in rooms:
        rooms = rooms.split(" ")
        x.append(1000 * int(rooms[1][0]))
    elif "Свободная" in rooms:
        rooms = rooms.split(" ")
        x.append(1000 * int(rooms[2][1]))
    elif "доля" in rooms:
        x.append(700)
    else:
        print(rooms)

    y = flat["cost_square_meter"].popitem()[1]
    return x, y


def create_model(flats, is_test=True):
    model = sklearn.linear_model.SGDRegressor(max_iter=2000, tol=3)
    x_train = []
    y_train = []
    for flat in flats:
        x, y = get_xy(flat)
        if x and y:
            x_train.append(x)
            y_train.append(y)
    scaler = StandardScaler()
    if is_test:
        scaler.fit(x_train)
        x_train = scaler.transform(x_train)
        model.fit(x_train, y_train)
        dump(scaler, SCALER_FILENAME)
    else:
        x_train, x_test, y_train, y_test = train_test_split(x_train, y_train, test_size=0.33, random_state=42)
        scaler.fit(x_train)
        x_train = scaler.transform(x_train)
        x_test = scaler.transform(x_test)

        model.fit(x_train, y_train)
        print(model.score(x_test, y_test))
        dump(scaler, SCALER_FILENAME)
    return model


def save_model(model):
    dump(model, model_FILENAME)


def load_model():
    try:
        return load(model_FILENAME)
    except FileNotFoundError:
        print("Create new MLP pls")
        return None


if __name__ == "__main__":
    train = DB.flats.find({})
    m = create_model(train, is_test=False)
    save_model(m)
    test = DB.flats.find({}).skip(10000)
    for test_flat in test:
        predicted_cost, real_cost = predict_cost(m, test_flat, test_mode=True)
        #print("Predicted:", predicted_cost, "\nReal:", real_cost, "\n", test_flat.get("url"))
