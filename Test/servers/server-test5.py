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
    print("Starting growing server")
    server = Server()
    server.set_endpoint("opc.tcp://0.0.0.0:4844/freeopcua/server/")

    # setup our own namespace, not really necessary but should as spec
    uri = "http://examples.freeopcua.github.io"
    idx = server.register_namespace(uri)

    # get Objects node, this is where we should put our custom stuff
    objects = server.get_objects_node()

    # populating our address space
    myobj = objects.add_object(idx, "MyObjectAddDirect")
    myobjref = objects.add_object(idx, "MyObjectAddRef")
    myobjexclude = objects.add_object(idx, "EXCLUDEMyObject")

    mybool = myobj.add_variable(idx, "MyVariable bool", ua.Variant(False, ua.VariantType.Boolean))
    mybool.set_writable()
    # Configure server to use sqlite as history database (default is a simple memory dict)
    server.iserver.history_manager.set_storage(HistorySQLite("my_datavalue_history5.sql"))

    # starting!
    server.start()

    auditNodeEmitter = server.get_event_generator(ua.ObjectIds.AuditAddNodesEventType)
    auditRefEmitter = server.get_event_generator(ua.ObjectIds.AuditAddReferencesEventType)

        
    try:
        count = 0
        while True:
            time.sleep(2)
            # add object and variable, notify only on myobj,
            newobj = myobj.add_object(idx, "AddObject " + str(count))
            objnew = ua.AddNodesItem()
            objnew.ParentNodeId = myobj.nodeid
            objnew.NodeClass = ua.NodeClass.Object
            objnew.TypeDefinition = ua.NodeId(ua.ObjectIds.BaseObjectType)
            
            auditNodeEmitter.event.NodesToAdd = ua.Variant([objnew], ua.VariantType.ExtensionObject)
            auditNodeEmitter.trigger(message="audit add1 " + str(count))
            
            # In most cases this won't trigger a re-browse, because the two happen
            # too close. This is fine however, as either way it should be 
            # caught by the next browse round.
            # In theory it is possible to miss one, but it is unlikely, and this was never
            # intended to be 100% precise.
            newvar = newobj.add_variable(idx, "AddVariable " + str(count), ua.Variant(0, ua.VariantType.Double))
            varnew = ua.AddNodesItem()
            varnew.ParentNodeId = newobj.nodeid
            varnew.NodeClass = ua.NodeClass.Variable
            varnew.TypeDefinition = ua.NodeId(ua.ObjectIds.BaseDataVariableType)
            auditNodeEmitter.event.NodesToAdd = ua.Variant([varnew], ua.VariantType.ExtensionObject)
            auditNodeEmitter.trigger(message="audit add2 " + str(count))

            # add variable to excluded object, notify, then add reference and notify
            # Expected behavior is that rebrowse is triggered on only the added reference
            newvar2 = myobjexclude.add_variable(idx, "AddExtraVariable " + str(count), ua.Variant(0, ua.VariantType.Double))
            varnew2 = ua.AddNodesItem()
            varnew2.ParentNodeId = myobjexclude.nodeid
            varnew2.NodeClass = ua.NodeClass.Variable
            varnew2.TypeDefinition = ua.NodeId(ua.ObjectIds.BaseDataVariableType)
            auditNodeEmitter.event.NodesToAdd = ua.Variant([varnew2], ua.VariantType.ExtensionObject)
            auditNodeEmitter.trigger()

            myobjref.add_reference(newvar2, ua.ObjectIds.HasComponent)
            refnew = ua.AddReferencesItem()
            refnew.SourceNodeId = myobjref.nodeid
            refnew.ReferenceTypeId = ua.NodeId(ua.ObjectIds.HasComponent)
            refnew.IsForward = True
            auditRefEmitter.event.ReferencesToAdd = ua.Variant([refnew], ua.VariantType.ExtensionObject)
            auditRefEmitter.trigger()

            count += 1
    finally:
        # close connection, remove subscriptions, etc
        server.stop()
