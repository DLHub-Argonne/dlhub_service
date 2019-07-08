import threading
import zmq_broker

from config import SECRET_KEY

from flask import Flask
from app.api.automate_api import automate_api
from app.api.views import api
from app.main.views import main
import logging

app = Flask(__name__)

app.register_blueprint(api, url_prefix="/api/v1")
app.register_blueprint(main)
app.register_blueprint(automate_api, url_prefix="/automate")

def start_broker():
    """
    Start the ZMQ broker. This allows multiple workers to submit requests.
    """
    try:
        broker = zmq_broker.ZMQBroker()
        broker.start(50000)
    except Exception as e:
        print("Broker failed. %s" % e)
        print("Continuing without a broker.")


app.secret_key = SECRET_KEY
app.config['SESSION_TYPE'] = 'filesystem'


if __name__ == "__main__":
    broker_thread = threading.Thread(name='broker_thread', target=start_broker, daemon=True)
    broker_thread.start()
    app.run()
else:
    gunicorn_logger = logging.getLogger('gunicorn.error')
    app.logger.handlers = gunicorn_logger.handlers
    app.logger.setLevel(gunicorn_logger.level)
    broker_thread = threading.Thread(name='broker_thread', target=start_broker, daemon=True)
    broker_thread.start()
