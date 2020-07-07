import math
import sys
import time
import os

sys.path.insert(1, os.path.join(os.path.dirname(__file__), '../../../python-opcua/build/lib/'))

from opcua import Server, ua
from opcua.server.history_sql import HistorySQLite

sys.path.insert(0, "..")




if __name__ == "__main__":

    # setup our server
    print("Start very large server")
    server = Server()
    server.set_endpoint("opc.tcp://0.0.0.0:4840/freeopcua/server/")

    # setup our own namespace, not really necessary but should as spec
    uri = "opc.tcp://test.localhost"
    idx = server.register_namespace(uri)

    # get Objects node, this is where we should put our custom stuff
    objects = server.get_objects_node()

    variables = []

    # populating our address space
    for i in range(0, 25):
        obj1 = objects.add_object(idx, "Object " + str(i))
        for j in range(0, 25):
            obj2 = obj1.add_object(idx, "Object " + str(i) + ", " + str(j))
            for l in range(0, 10):
                obj3 = obj2.add_object(idx, "Object " + str(i) + ", " + str(j) + ", " + str(l))
                for k in range(0, 2):
                    variables.append(obj3.add_variable(idx, "Variable " + str(i) + ", " + str(j) + ", " + str(k), ua.Variant(0, ua.VariantType.Double)))



    # starting!
    server.start()

    # enable data change history for this particular node, must be called after start since it uses subscription
    # server.historize_node_data_change(myvar, count=10000)
    # server.historize_node_data_change(mybool, count=1000)

    try:
        count = 0
        bcount = 0
        while True:
            time.sleep(1)
            count += 1
            for i in range(0, len(variables)):
                variables[i].set_value(count)


    finally:
        # close connection, remove subscriptions, etc
        server.stop()
