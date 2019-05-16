from celery import Celery
from datetime import timedelta
app = Celery('kek', broker='amqp://admin:admin@localhost:5672/parserhost')

app.conf.beat_schedule = {
    'say-hello-every-30-seconds': {
        'task': 'tasks.say_hello',
        'schedule': timedelta(seconds=30),
        'args': ["Blog reader"]
    },
}
CELERY_TIMEZONE = 'UTC'


@app.task
def say_hello(name):
    print("Hello, {}".format(name))
