from .types import *


class Column:
    def __init__(
        self, column_type: TablestoreType, primary_key=False, index=False, default=None
    ):
        assert issubclass(column_type, TablestoreType)
        if primary_key:
            assert issubclass(column_type, (String, Integer, Binary))

        self.column_type = column_type
        self.primary_key = primary_key
        self.index = index
        self.default = default

    def __repr__(self):
        return f"<Column({self.column_type})>"
