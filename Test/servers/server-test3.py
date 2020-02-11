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
    print("Starting array server")
    server = Server()
    server.set_endpoint("opc.tcp://0.0.0.0:4842/freeopcua/server/")

    # setup our own namespace, not really necessary but should as spec
    uri = "http://examples.freeopcua.github.io"
    idx = server.register_namespace(uri)

    # get Objects node, this is where we should put our custom stuff
    objects = server.get_objects_node()

    # populating our address space
    myobj = objects.add_object(idx, "MyObject")
    
    myvar = myobj.add_variable(idx, "MyArray", ua.Variant([1.0, 2.0, 0, 0], ua.VariantType.Double))
    myvar.set_array_dimensions([4])
    myvar.set_writable()  # Set MyVariable to be writable by clients

    myvar2 = myobj.add_variable(idx, "MyStringArray", ua.Variant(["word", "word2"], ua.VariantType.String))
    myvar2.set_writable()
    myvar2.set_array_dimensions([2])

    mystring = myobj.add_variable(idx, "MyString", ua.Variant(None, ua.VariantType.String))
    mystring.set_writable()  # Set to be writable by clients

    myvar.add_property(idx, "TS property 1", ua.Variant("test", ua.VariantType.String))
    myvar.add_property(idx, "TS property 2", ua.Variant(123.20, ua.VariantType.Double))

    myobj.add_property(idx, "Asset prop 1", ua.Variant("test", ua.VariantType.String))
    myobj.add_property(idx, "Asset prop 2", ua.Variant(123.21, ua.VariantType.Double))

    # Custom data types
    # Normal string-parseable datatype
    stringyType = server.create_custom_data_type(idx, "StringyType", ua.ObjectIds.BaseDataType)
    stringyType.set_attribute(ua.AttributeIds.NodeClass, ua.DataValue(ua.NodeClass.DataType))
    stringyType.set_attribute(ua.AttributeIds.IsAbstract, ua.DataValue(True))
    
    # Non-specific type, to be ignored
    ignoreType = server.create_custom_data_type(idx, "IgnoreType", ua.ObjectIds.BaseDataType)
    ignoreType.set_attribute(ua.AttributeIds.NodeClass, ua.DataValue(ua.NodeClass.DataType))
    ignoreType.set_attribute(ua.AttributeIds.IsAbstract, ua.DataValue(True))
    # Numeric type, should be autodetected
    numberType = server.create_custom_data_type(idx, "MyNumeric", ua.ObjectIds.Number)
    numberType.set_attribute(ua.AttributeIds.NodeClass, ua.DataValue(ua.NodeClass.DataType))
    numberType.set_attribute(ua.AttributeIds.IsAbstract, ua.DataValue(True))
    # Also numeric type, should be autodetected, outside of Number node
    numberType2 = server.create_custom_data_type(idx, "MyNumber2", ua.ObjectIds.BaseDataType)
    numberType2.set_attribute(ua.AttributeIds.NodeClass, ua.DataValue(ua.NodeClass.DataType))
    numberType2.set_attribute(ua.AttributeIds.IsAbstract, ua.DataValue(True))

    # Nodes with custom types and misc properties
    # Root
    customRoot = objects.add_object(idx, "CustomTypesRoot")
    # StringyType node
    strVar = customRoot.add_variable(idx, "StringyVar", ua.Variant("stringy 0", ua.VariantType.String), ua.VariantType.String, stringyType.nodeid)
    # IgnoreType node
    ignoreVar = customRoot.add_variable(idx, "IgnoreTypeVar", ua.Variant("ignore 0", ua.VariantType.String), ua.VariantType.String, ignoreType.nodeid)
    # Numeric type node
    numberVar = customRoot.add_variable(idx, "NumericTypeVar", ua.Variant(0.0, ua.VariantType.Double), ua.VariantType.Double, numberType.nodeid)
    # Numeric type 2 node
    numberVar2 = customRoot.add_variable(idx, "NumericTypeVar2", ua.Variant(0.0, ua.VariantType.Double), ua.VariantType.Double, numberType.nodeid)

    # Add some interesting properties to one of the numeric nodes, using built-in types
    euinf = ua.EUInformation()
    euinf.DisplayName = ua.LocalizedText("°C")
    euinf.Description = ua.LocalizedText("degree Celsius")
    euinf.NamespaceUri = "http://www.opcfoundation.org/UA/units/un/cefact"
    euinf.UnitId = 4408652
    numberVar.add_property(idx, "EngineeringUnits", ua.Variant(euinf, ua.VariantType.ExtensionObject), ua.VariantType.ExtensionObject, ua.ObjectIds.EUInformation)

    rng = ua.Range()
    rng.Low = 0
    rng.High = 100
    numberVar.add_property(idx, "EURange", ua.Variant(rng, ua.VariantType.ExtensionObject), ua.VariantType.ExtensionObject, ua.ObjectIds.Range)


    # Configure server to use sqlite as history database (default is a simple memory dict)
    server.iserver.history_manager.set_storage(HistorySQLite("my_datavalue_history_3.sql"))

    # starting!
    server.start()

    # enable data change history for this particular node, must be called after start since it uses subscription
    server.historize_node_data_change(myvar, count=10000)
    server.historize_node_data_change(numberVar, count=10000)
    # server.historize_node_data_change(mybool, count=1000)

    try:
        count = 0
        bcount = 0
        while True:
            time.sleep(0.2)
            count += 0.1
            bcount += 1
            myvar.set_value([math.sin(count), math.cos(count), bcount, -1 * bcount])
            myvar2.set_value(["word: " + str(bcount), "word2: " + str(bcount)])
            strVar.set_value("stringy " + str(bcount))
            ignoreVar.set_value("ignore " + str(bcount))
            numberVar.set_value(bcount)
            numberVar2.set_value(bcount)

    finally:
        # close connection, remove subscriptions, etc
        server.stop()
