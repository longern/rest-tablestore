import pickle
import re
from itertools import chain

import tablestore

from .models import Model


def row_as_dict(row: tablestore.Row) -> dict:
    row_dict = {}
    for item in chain(row.primary_key, row.attribute_columns):
        key, value, *_ = item
        row_dict[key] = value if not isinstance(value, bytearray) else pickle.loads(value)
    return row_dict


class TablestoreQuery:
    def __init__(self, session: "Session", model: Model):
        self.session = session
        self.model = model

    def all(self):
        client = self.session.client
        consumed, next_start_primary_key, row_list, _ = client.get_range(
            self.model.__table__.name,
            "FORWARD",
            [("id", tablestore.INF_MIN)],
            [("id", tablestore.INF_MAX)],
            max_version=1,
        )

        rows = []
        for row in row_list:
            rows.append(self.model(**row_as_dict(row)))
        return rows

    def filter(self, **kwargs):
        assert len(kwargs) == 1
        key = next(iter(kwargs))
        assert key in self.model.__table__.columns and self.model.__table__.columns[key].index

        _, _, rows, _ = self.session.client.get_range(
            f"ix_{self.model.__table__.name}_{key}",
            "FORWARD",
            [(key, kwargs[key]), ("id", tablestore.INF_MIN)],
            [(key, kwargs[key]), ("id", tablestore.INF_MAX)],
            max_version=1,
        )

        if not rows:
            return None

        return [self.model(**row_as_dict(row)) for row in rows]

    def get(self, pk):
        primary_keys = self.model.__table__.primary_keys
        if not isinstance(pk, dict) and len(primary_keys) == 1:
            pk = {next(iter(primary_keys)): pk}

        assert isinstance(pk, dict) and set(pk.keys()) == set(primary_keys.keys())

        client = self.session.client
        _, row, _ = client.get_row(self.model.__table__.name, list(pk.items()), max_version=1)

        if not row:
            return None

        return self.model(**row_as_dict(row))


class Session:
    def __init__(self, database_uri: str):
        matches = re.match("tablestore://([^:]*):([^@]*)@([^/]*)/([^?]*)", database_uri)
        username, password, hostname, instance = matches.groups()
        self.client = tablestore.OTSClient(
            "https://" + hostname,
            username,
            password,
            instance,
        )

    def query(self, model: Model):
        return TablestoreQuery(self, model)

    def table_names(self):
        return self.client.list_table()
