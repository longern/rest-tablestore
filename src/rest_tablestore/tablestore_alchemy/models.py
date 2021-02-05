import re

from .column import Column


class ModelTable:
    def __init__(self, cls):
        self.cls = cls

    @property
    def name(self) -> str:
        if hasattr(self.cls, "__tablename__") and isinstance(self.cls.__tablename__, str):
            return self.cls.__tablename__

        return "".join(map(str.lower, re.findall("[A-Z][^A-Z]*", self.cls.__name__)))

    @property
    def columns(self) -> dict:
        return {key: val for key, val in self.cls.__dict__.items() if isinstance(val, Column)}

    @property
    def primary_keys(self) -> dict:
        return {key: val for key, val in self.columns.items() if val.primary_key}


class Model:
    def __init__(self, **kwargs):
        for key, val in kwargs.items():
            setattr(self, key, val)

    def __init_subclass__(cls):
        super().__init_subclass__()
        cls.__table__ = ModelTable(cls)

    def __repr__(self):
        return self.__class__.__name__ + str(self.__dict__)
