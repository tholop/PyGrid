import syft as sy
import torch as th
from flask import Blueprint

import logging

from .. import db, BaseModel, executor
from .flight_server import FlightServer

# Avoid Pytorch deadlock issues
th.set_num_threads(1)

hook = sy.TorchHook(th)
local_worker = sy.VirtualWorker(hook, auto_add=False)
hook.local_worker.is_client_worker = False

main_routes = Blueprint("main", __name__)
model_centric_routes = Blueprint("model-centric", __name__)
data_centric_routes = Blueprint("data-centric", __name__)
ws = Blueprint(r"ws", __name__)
# ws_arrow = Blueprint(r"ws_arrow", __name__)

# Blocking?
# TODO: get port
# scheme = "grpc+tcp"
# host = "0.0.0.0"
# port = "7605"
# location = "{}://{}:{}".format(scheme, host, port)
# flight = FlightServer(host, location, local_worker=local_worker)
# logging.info("Starting flight")
# flight.serve()

logging.info("Never reached")

from . import events, routes
from .data_centric import auth
