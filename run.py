import connexion
import threading
import zmq_broker

from config import SECRET_KEY

from app.api.views import api
from app.main.views import main

app = connexion.FlaskApp(__name__, specification_dir='openapi/')
app.add_api('api.yaml')
app.register_blueprint(api, url_prefix="/api/v1")
app.register_blueprint(main)


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
    broker_thread = threading.Thread(name='broker_thread', target=start_broker, daemon=True)
    broker_thread.start()
