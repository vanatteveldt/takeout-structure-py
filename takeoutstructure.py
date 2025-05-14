import csv
import json
import logging
import sys
from typing import IO, Iterable, TextIO, Tuple

d = json.loads(open("/home/wva/Downloads/user_data_tiktok.json").read())


def get_json_structure(json_data, prefix: list, mask_primitives=True) -> Iterable[Tuple[list, str]]:
    # Recursivly iterate through the json structure and yield indicators of structure
    if isinstance(json_data, list):
        # Recursive call for each element, prefixed with index
        for i, subval in enumerate(json_data):
            yield from get_json_structure(subval, prefix + [i], mask_primitives=mask_primitives)
    elif isinstance(json_data, dict):
        # Recursive call for each element, prefixed with key
        for subkey, subval in json_data.items():
            yield from get_json_structure(subval, prefix + [subkey], mask_primitives=mask_primitives)
    else:
        # Yield structure and data type
        leaf = type(json_data).__name__ if mask_primitives else json_data
        yield prefix, leaf


def change_values_to_type(json_data):
    # Recursivly iterate through the json structure and change any leaf nodes to a string indicating their type
    if isinstance(json_data, list):
        for i, subval in enumerate(json_data):
            json_data[i] = change_values_to_type(subval)
        return json_data
    elif isinstance(json_data, dict):
        for key, subval in json_data.items():
            json_data[key] = change_values_to_type(subval)
        return json_data
    else:
        return type(json_data).__name__


def get_structure_from_file(file: IO) -> Iterable[Tuple[list | dict | str, list, str]]:
    json_data = json.load(file)
    if not isinstance(json_data, dict):
        # Note: could change logic to also accept primitives or lists, but not sure there's a need
        raise ValueError("First level should be a dict")
    for key, subtree in json_data.items():
        treestruct = change_values_to_type(subtree)
        for prefix, datatype in get_json_structure(treestruct, [], mask_primitives=False):
            yield {key: treestruct}, [key] + prefix, datatype


def structure_to_csv(rows: Iterable[Tuple[list | dict | str, list, str]], outfile: TextIO = sys.stdout, ncolumns=7):
    w = csv.writer(outfile)
    w.writerow(["treestructure"] + [f"col_{x}" for x in range(1, ncolumns + 1)] + ["datatype"])
    for struct, path, datatype in rows:
        # Check for overly long paths
        if len(path) > ncolumns:
            logging.warning(f"Path ({len(path)}) is longer than ncolumns ({ncolumns}), merging tail")
            path = path[: (ncolumns - 1)] + [".".join(path[(ncolumns - 1) :])]
        # Add trailing Nones if needed
        path = path + [None] * (ncolumns - len(path))
        w.writerow([json.dumps(struct)] + path + [datatype])


if __name__ == "__main__":
    json_structure = get_structure_from_file(sys.stdin)
    structure_to_csv(json_structure, outfile=sys.stdout)
