Note, you will need permissions to edit windows services for this tool to work.

This tool can create new OpcUa extractor services, to extract data from multiple OpcUa servers using one server.
The installer creates a default service, which you will not see using this tool.

It will check for a registry entry made by the installer and try and locate the OpcuaExtractor.exe
If this file isn't found you won't be able to create new services.

Explanation of the tool:

Extractor Services: Lists services created by this tool, finds all services with name starting with "opcuaext"
Delete Service:		Will delete the selected service after confirmation

Create new service:
Name:				Display name, name of service as displayed in 'services.msc' - Need to be unique.
					This name will also be used as prefix in eventlog messages from the service.
Description:		Add some description about the OpcUa service this extractor is connected too.
Working Dir:		Need to be unique for each service you create, it will also require a config folder inside it
					with a config.yml file unique to this extractor service.

Create Service:		If information provided above is correct will create a new windows service.

The newly created service will have no restart options and start type: Manual

Now is the time to create a config\config.yml inside the 'Working Dir' specified above, start the service.
You also need to copy over the 'Certificates' folder from the main extractor folder to 'Working Dir'
After starting it: inspect the Application eventlog and also the log file inside the 'logs' folder created by the extractor.

Once you have verified its working, set service startup to 'Automatic (Delayed Start)' and under the Recovery tab set all 3
failure options to 'Restart the Service'

Example folder structure for 3 services, which would each have their own 'extractorX' folder as working dir.
If possible try and use another disk other than C: which is usually the OS disk.

D:\OPCUAEXTRACTOR
├───extractor1
│   ├───certificates
│   ├───config\config.yml
│   └───logs
├───extractor2
│   ├───certificates
│   ├───config\config.yml
│   └───logs
└───extractor3
│   ├───certificates
    ├───config\config.yml
    └───logs

