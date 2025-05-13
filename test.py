import json

d = json.loads(open("/home/wva/Downloads/user_data_tiktok.json").read())


def structure(x, prefix):
    if isinstance(x, list):
        for i, subval in enumerate(x):
            yield from structure(subval, prefix + [i])
    elif isinstance(x, dict):
        for subkey, subval in x.items():
            yield from structure(subval, prefix + [subkey])
    else:
        yield prefix, type(x).__name__


import csv


COLUMNS = 7
w = csv.writer(open("/tmp/test.csv", "w"))
w.writerow([f"col{x}" for x in range(1, COLUMNS + 1)] + ["datatype"])
for path, typename in structure(d, []):
    path = path + ([None] * (COLUMNS - len(path)))
    w.writerow(path + [typename])
