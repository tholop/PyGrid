import argparse
import ast
import threading
import time

import numpy as np
import pyarrow
import pyarrow.flight

import logging


class FlightServer(pyarrow.flight.FlightServerBase):
    def __init__(
        self,
        host="localhost",
        location=None,
        tls_certificates=None,
        verify_client=False,
        root_certificates=None,
        auth_handler=None,
        local_worker=None,
    ):
        super(FlightServer, self).__init__(
            location, auth_handler, tls_certificates, verify_client, root_certificates
        )
        self.flights = {}
        self.host = host
        self.tls_certificates = tls_certificates

    def do_put(self, context, descriptor, reader, writer):
        key = FlightServer.descriptor_to_key(descriptor)
        t = time.time()
        print(f"Got a new key: {key}")

        table = reader.read_all()
        # n = np.asarray(table.to_pandas())
        # self.flights[key] = reader.read_all()
        dl_time = time.time() - t
        print("Read all and converted to numpy:")
        n = np.asarray(table.to_pandas())
        # print(self.flights[key])
        print(n.shape)
        print(f"Time: {dl_time}")
        print(f"Bandwidth (Gb/s): {(n.nbytes / 1_000_000_000) / dl_time }")

        local_worker.swallow_numpy_array(n)


if __name__ == "__main__":
    scheme = "grpc+tcp"
    host = "0.0.0.0"
    port = "7605"
    location = "{}://{}:{}".format(scheme, host, port)
    flight = FlightServer(host, location)
    logging.info("Starting flight")
    flight.serve()