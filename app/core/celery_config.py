from celery import Celery
from app.application import APP


def make_celery():
    cel = Celery(APP.import_name, broker=APP.config['CELERY_BROKER_URL'],
                 backend=APP.config['CELERY_RESULT_BACKEND'])
    cel.conf.update(APP.config)

    class ContextTask(cel.Task):
        def __call__(self, *args, **kwargs):
            with APP.app_context():
                return self.run(*args, **kwargs)

    cel.Task = ContextTask
    return cel


cel = make_celery()
