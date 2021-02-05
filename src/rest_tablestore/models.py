from uuid import uuid4

from .tablestore_alchemy.column import *
from .tablestore_alchemy.models import Model
from .tablestore_alchemy.session import Session


class User(Model):
    __tablename__ = "users"
    id = Column(String, primary_key=True, default=uuid4)
    username = Column(String, index=True)
    password = Column(String)


class RestTablestoreModel(Model):
    __tablename__ = "rest_tablestore_models"
    id = Column(String, primary_key=True, default=uuid4)
    model = Column(String)
    column = Column(String)
    to = Column(String)
    default = Column(String)


def dynamic_model(session: Session, table_name: str, model_name=None) -> Model:
    column_type_mapping = {
        "STRING": String,
        "INTEGER": Integer,
        "BINARY": Binary,
        "DOUBLE": Float,
        "BOOLEAN": Boolean,
    }
    if not model_name:
        model_name = "".join(word.capitalize() for word in table_name.split("_"))

    table_meta = session.client.describe_table(table_name).table_meta
    columns_dict = {
        column_name: Column(column_type_mapping[column_type], primary_key=True)
        for column_name, column_type in table_meta.schema_of_primary_key
    }
    columns_dict.update(
        (column_name, Column(column_type_mapping[column_type]))
        for column_name, column_type in table_meta.defined_columns
    )

    return type(model_name, (Model,), {"__tablename__": table_name, **columns_dict})
