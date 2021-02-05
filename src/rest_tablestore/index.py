#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import hashlib
import logging
import os
import pickle
import re
import time
import uuid
import functools

import tablestore
from flask import Flask, request, g
from werkzeug.exceptions import HTTPException, MethodNotAllowed, NotFound, Unauthorized, UnprocessableEntity
from werkzeug.security import gen_salt, generate_password_hash, check_password_hash

from .models import User, RestTablestoreModel, dynamic_model
from .tablestore_alchemy.session import Session

app = Flask(__name__)
app.config["JSON_SORT_KEYS"] = False
app.config["SECRET_KEY"] = os.getenv("SECRET_KEY", "f5xl2fa-=5-lt1n!ucyzv%kz(t=2p4u#tqr$7!vj*a*wmiufr#")
session = Session(os.getenv("DATABASE"))


def get_ots_client() -> tablestore.OTSClient:
    return session.client


@app.route("/")
def welcome():
    return {"welcome": "hello!"}


@app.route("/site")
def site():
    return {
        "locale": "zh-CN",
        "menu": [{"name": "Home", "url": "/"}]
        + [
            {
                "name": table_name.capitalize().replace("_", " "),
                "url": f"/rest/{table_name}",
            }
            for table_name in session.table_names()
            if not table_name.startswith("rest_tablestore_")
        ]
        + [{"name": "logout", "url": "/login"}],
    }


@app.route("/home")
def home():
    return {}


@app.route("/login", methods=["POST"])
def login():
    body: dict = request.json
    logging.info("Logging username: %s", body.get("username"))

    if not body.get("username"):
        raise UnprocessableEntity([{"field": "username", "message": "Empty username."}])

    users = session.query(User).filter(username=body.get("username"))

    if not users:  # User not found
        raise UnprocessableEntity([{"field": "username", "message": "User not found."}])

    if not check_password_hash(users[0].password, body.get("password")):
        raise UnprocessableEntity([{"field": "password", "message": "Incorrect password."}])

    timestamp = int(time.time())
    salt = gen_salt(8)
    token = hashlib.sha256(f"{users[0].id}{timestamp}{salt}{app.config['SECRET_KEY']}".encode()).hexdigest()

    return {"token": f"{users[0].id},{timestamp},{salt},{token}"}


@app.route("/login", methods=["GET"])
def login_get_not_allowed():
    raise MethodNotAllowed(["POST"])


def login_required(func):
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        match = re.match(
            "Bearer ([^,]*),([^,]*),([^,]*),([^,]*)",
            request.headers.get("Authorization", ""),
        )
        if not match:
            raise Unauthorized("Invalid authorization header.")

        user_id, timestamp, salt, token = match.groups()
        expected_token = hashlib.sha256(f"{user_id}{timestamp}{salt}{app.config['SECRET_KEY']}".encode()).hexdigest()
        if token != expected_token or time.time() - int(timestamp) >= 86400:
            raise Unauthorized("Wrong or expired token.")

        g.user_id = user_id

        return func(*args, **kwargs)

    return wrapper


@app.route("/<obj>/grid")
def grid(obj: str):
    columns = dynamic_model(session, obj).__table__.columns
    return {"fields": {column_name: {} for column_name, column_type in columns.items()}}


@app.route("/<obj>/form")
def form(obj: str):
    columns = dynamic_model(session, obj).__table__.columns
    return {
        "fields": {
            column_name: {"type": "html" if column_name == "content" else "text"}
            for column_name, column_type in columns.items()
        }
    }


@app.route("/<obj>")
def list_objects(obj: str):
    rows = session.query(dynamic_model(session, obj)).all()
    return {"data": [row.__dict__ for row in rows]}


@app.route("/<obj>", methods=["POST"])
@app.route("/<obj>/<id>", methods=["POST", "PUT"])
@login_required
def update_object(obj: str, id=None):
    body: dict = request.json
    if "password" in body:
        body["password"] = generate_password_hash(body["password"])

    metas = [model for model in session.query(RestTablestoreModel).all() if model.model == obj]
    defaults = {meta.column: meta.default for meta in metas if getattr(meta, "default", None)}

    id = str(uuid.uuid4()) if not id else id

    if "id" in body:
        del body["id"]
    for key, value in body.items():
        if isinstance(value, (list, dict)):
            body[key] = bytearray(pickle.dumps(value))
    for column_name, default_value in defaults.items():
        if column_name not in body:
            body[column_name] = default_value.format(request=request, g=g)

    client = get_ots_client()
    _, row = client.put_row(
        obj,
        tablestore.Row([("id", id)], list(body.items())),
    )
    return request.args


@app.route("/<obj>/<id>")
@app.route("/<obj>/view")
def get_objects(obj: str, id=None):
    row = session.query(dynamic_model(session, obj)).get(request.args.get("id", id))
    if not row:
        raise NotFound()
    return row.__dict__


@app.route("/favicon.ico")
def get_favicon():
    raise NotFound()


@app.after_request
def after_request(response):
    response.headers.add("Access-Control-Allow-Origin", "*")
    response.headers.add("Access-Control-Allow-Headers", "Content-Type,Authorization")
    response.headers.add("Access-Control-Allow-Methods", "GET,PUT,POST,DELETE")
    return response


@app.errorhandler(HTTPException)
def handle_exception(exception: HTTPException):
    return {
        "name": exception.__class__.__name__,
        "message": exception.description,
    }, exception.code


if __name__ == "__main__":
    app.run(port=8088)
