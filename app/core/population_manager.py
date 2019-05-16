import csv

FILENAME = "/home/zviger/PycharmProjects/parser_realt.by/app/core/population_by_0.csv"

DATA_SET = {}

with open(FILENAME, "r") as file:
    reader = csv.reader(file)

    for row in reader:
        try:
            locations = DATA_SET[row[1]]
        except KeyError:
            locations = {}
        locations[row[2]] = row[3]
        DATA_SET[row[1]] = locations


if __name__ == "__main__":
    print(DATA_SET)
