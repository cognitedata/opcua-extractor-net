import math
import sys
import time
import os


sys.path.insert(1, os.path.join(os.path.dirname(__file__), '../../../python-opcua/build/lib/'))

from datetime import datetime
from opcua import Server, ua
from opcua.server.history_sql import HistorySQLite

sys.path.insert(0, "..")




if __name__ == "__main__":

    # setup our server
    print("Starting events server")
    server = Server()
    server.set_endpoint("opc.tcp://0.0.0.0:4843/freeopcua/server/")

    # setup our own namespace, not really necessary but should as spec
    uri = "http://examples.freeopcua.github.io"
    idx = server.register_namespace(uri)

    # get Objects node, this is where we should put our custom stuff
    objects = server.get_objects_node()

    # populating our address space
    myobj = objects.add_object(idx, "MyObject")
    myobj2 = objects.add_object(idx, "MyObject2")
    myobjexclude = objects.add_object(idx, "EXCLUDEMyObject")
    myvar = myobj.add_variable(idx, "MyVariable", ua.Variant(0, ua.VariantType.Double))
    myvar.set_writable()  # Set MyVariable to be writable by clients

    myvar2 = myobj.add_variable(idx, "MyVariable 2", ua.Variant(0, ua.VariantType.Double))
    myvar.set_writable()  # Set MyVariable to be writable by clients

    mystring = myobj.add_variable(idx, "MyString", ua.Variant(None, ua.VariantType.String))
    mystring.set_writable()  # Set to be writable by clients

    myvar.add_property(idx, "TS property 1", ua.Variant("test", ua.VariantType.String))
    myvar.add_property(idx, "TS property 2", ua.Variant(123.20, ua.VariantType.Double))

    myobj.add_property(idx, "Asset prop 1", ua.Variant("test", ua.VariantType.String))
    myobj.add_property(idx, "Asset prop 2", ua.Variant(123.21, ua.VariantType.Double))

    mybool = myobj.add_variable(idx, "MyVariable bool", ua.Variant(False, ua.VariantType.Boolean))
    # Configure server to use sqlite as history database (default is a simple memory dict)
    server.iserver.history_manager.set_storage(HistorySQLite("my_datavalue_history4.sql"))

    # starting!
    server.start()

    

    eventPropType = server.create_custom_event_type(2, 'EventExtraProperties', ua.ObjectIds.BaseEventType, [('PropertyNum', ua.VariantType.Float), ('PropertyString', ua.VariantType.String), ("SubType", ua.VariantType.String)])
    eventBasicType1 = server.create_custom_event_type(2, 'EventBasic1', ua.ObjectIds.BaseEventType)
    eventBasicType2 = server.create_custom_event_type(2, 'EventBasic2', ua.ObjectIds.BaseEventType)
    eventCustomType = server.create_custom_event_type(2, 'EventCustomType', ua.ObjectIds.BaseEventType, [('TypeProp', ua.VariantType.String)])


    # Test basic getting of events with properties
    propEmitter = server.get_event_generator(eventPropType)
    propEmitter.event.SourceNode = myobj.nodeid
    propEmitter.event.PropertyNum = 1
    propEmitter.event.SubType = "TestSubType"
    propEmitter.event.PropertyString = "TestString 0"
    # Test getting of events from other emitter
    propEmitterOther = server.get_event_generator(eventPropType, myobj)
    propEmitterOther.event.SourceNode = myobj.nodeid
    propEmitterOther.event.PropertyNum = 1
    propEmitterOther.event.SubType = "TestSubType"
    propEmitterOther.event.PropertyString = "TestStringOther 0"
    propEmitterOther.event.SourceName = myobj.get_display_name().Text
    # Test filtering out events from third emitter
    propEmitterOther2 = server.get_event_generator(eventPropType, myobj2)
    propEmitterOther2.event.SourceNode = myobj.nodeid
    propEmitterOther2.event.SourceName = myobj.get_display_name().Text
    # Test getting of second event type from same emitter
    basicPassEmitter = server.get_event_generator(eventBasicType1)
    basicPassEmitter.event.SourceNode = myobj.nodeid
    basicPassEmitter.event.SourceName = myobj.get_display_name().Text
    # Test filtering out event types
    basicBlockEmitter = server.get_event_generator(eventBasicType2)
    basicBlockEmitter.event.SourceNode = myobj.nodeid
    basicBlockEmitter.event.SourceName = myobj.get_display_name().Text
    # Test with different source nodes
    basicPassSourceEmitter = server.get_event_generator(eventBasicType1)
    basicPassSourceEmitter.event.SourceNode = myobj2.nodeid
    basicPassSourceEmitter.event.SourceName = myobj2.get_display_name().Text
    
    basicPassSourceEmitter2 = server.get_event_generator(eventBasicType1, myobj)
    basicPassSourceEmitter2.event.SourceNode = myobj2.nodeid
    basicPassSourceEmitter2.event.SourceName = myobj2.get_display_name().Text
    
    basicVarSourceEmitter = server.get_event_generator(eventBasicType1)
    basicVarSourceEmitter.event.SourceNode = myvar.nodeid
    basicVarSourceEmitter.event.SourceName = myvar.get_display_name().Text
    
    basicNoSourceEmitter = server.get_event_generator(eventBasicType1)
    
    basicExcludeSourceEmitter = server.get_event_generator(eventBasicType1)
    basicExcludeSourceEmitter.event.SourceNode = myobjexclude.nodeid 
    basicExcludeSourceEmitter.event.SourceName = myobjexclude.get_display_name().Text

    mappedTypeEmitter = server.get_event_generator(eventCustomType)
    mappedTypeEmitter.event.TypeProp = "MySpecialType"
    mappedTypeEmitter.event.SourceNode = myobj.nodeid
    mappedTypeEmitter.event.SourceName = myobj.get_display_name().Text

    # Make server node event-historizing
    server_node = server.get_node(ua.ObjectIds.Server)
    server.historize_node_event(server_node, period=None)
    
    try:
        count = 0
        while True:
            time.sleep(1)
            myvar.set_value(math.sin(count))
            
            propEmitter.event.PropertyString = "TestString " + str(count)
            propEmitter.trigger(message="prop " + str(count))
            
            propEmitterOther.event.PropertyString = "TestStringOther " + str(count)
            propEmitterOther.trigger(message="propOther " + str(count))

            propEmitterOther2.trigger(message="propOther2 " + str(count))
            basicPassEmitter.trigger(message="basicPass " + str(count))
            basicBlockEmitter.trigger(message="basicBlock " + str(count))
            basicPassSourceEmitter.trigger(message="basicPassSource " + str(count))
            basicPassSourceEmitter2.trigger(message="basicPassSource2 " + str(count))
            basicVarSourceEmitter.trigger(message="basicVarSource " + str(count))
            basicNoSourceEmitter.trigger(message="basicNoSource " + str(count))
            basicExcludeSourceEmitter.trigger(message="basicExcludeSource " + str(count))
            mappedTypeEmitter.trigger(message="mappedType " + str(count))
            count += 1
    finally:
        # close connection, remove subscriptions, etc
        server.stop()
