from flask import Flask
from app.config import Configuration

APP = Flask(__name__)
APP.config.from_object(Configuration)
