class TablestoreTypeMeta(type):
    def __repr__(cls):
        return cls.__name__.upper()


class TablestoreType(metaclass=TablestoreTypeMeta):
    pass


class String(TablestoreType):
    pass


class Integer(TablestoreType):
    pass


class Binary(TablestoreType):
    pass


class FloatMeta(TablestoreTypeMeta):
    def __repr__(_):
        return "DOUBLE"


class Float(TablestoreType, metaclass=FloatMeta):
    pass


class Boolean(TablestoreType):
    pass
