import math
import sys
import time

from opcua import Server, ua
from opcua.server.history_sql import HistorySQLite

sys.path.insert(0, "..")




if __name__ == "__main__":

    # setup our server
    server = Server()
    server.set_endpoint("opc.tcp://0.0.0.0:4840/freeopcua/server/")

    # setup our own namespace, not really necessary but should as spec
    uri = "http://examples.freeopcua.github.io"
    idx = server.register_namespace(uri)

    # get Objects node, this is where we should put our custom stuff
    objects = server.get_objects_node()

    # populating our address space
    myobj = objects.add_object(idx, "MyObject")
    myvar = myobj.add_variable(idx, "MyVariable", ua.Variant(0, ua.VariantType.Double))
    myvar.set_writable()  # Set MyVariable to be writable by clients

    mystring = myobj.add_variable(idx, "MyString", ua.Variant(None, ua.VariantType.String))
    mystring.set_writable()  # Set to be writable by clients

    myvar.add_property(idx, "TS property 1", ua.Variant("test", ua.VariantType.String))
    myvar.add_property(idx, "TS property 2", ua.Variant(123.20, ua.VariantType.Double))

    myobj.add_property(idx, "Asset prop 1", ua.Variant("test", ua.VariantType.String))
    myobj.add_property(idx, "Asset prop 2", ua.Variant(123.21, ua.VariantType.Double))

    myobj2 = myobj.add_object(idx, "MyObject2")
    for i in range(0, 1200):
        myobj2.add_variable(idx, "MyVariable" + str(i), ua.Variant(0, ua.VariantType.Double))
        
    myobj3 = myobj.add_object(idx, "MyObject3")
    for j in range(0, 5):    
        mydeepobj = myobj3
        for i in range(0, 30):
            mydeepobj = mydeepobj.add_object(idx, "MyObject " + str(i) + ", " + str(j))
    
    mybool = myobj.add_variable(idx, "MyVariable bool", ua.Variant(False, ua.VariantType.Boolean))
    # Configure server to use sqlite as history database (default is a simple memory dict)
    server.iserver.history_manager.set_storage(HistorySQLite("my_datavalue_history.sql"))

    # starting!
    server.start()

    # enable data change history for this particular node, must be called after start since it uses subscription
    server.historize_node_data_change(myvar, count=10000)
    # server.historize_node_data_change(mybool, count=1000)

    try:
        count = 0
        bcount = 0
        while True:
            time.sleep(0.2)
            count += 0.1
            bcount += 1
            myvar.set_value(math.sin(count))
            if (bcount % 10 == 0):
                mybool.set_value(not mybool.get_value())
                

    finally:
        # close connection, remove subscriptions, etc
        server.stop()
