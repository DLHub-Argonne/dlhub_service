from parsl.app.app import python_app
import json
import time
import numpy as np

@python_app(cache=True, executors=['$executor'])
def dlhub_$function(data):
    start = time.time()
    global shim
    if 'shim' not in globals():
        from home_run import create_servable
        with open('dlhub.json') as fp:
            shim = create_servable(json.load(fp))
    x = shim.run(data)
    end = time.time()
    return (x, (end-start) * 1000)
