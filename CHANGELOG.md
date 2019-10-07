# OPC-UA Extractor Changelog
0.8.1  2019-10-07
------------------
* Config option for earliest history-read timestamp

0.8.0  2019-10-07
------------------
* Add support for reading events from OPC-UA and pushing to events in CDF.
* Update to .NET Core 3.0, trim and optimize binaries, and deploy as a single platform specific executable

0.7.0  2019-10-03
------------------
* Configurable chunk sizes to CDF and OPC UA server.
* Disable history synchronization with Source.History = false.

0.6.0  2019-10-01
------------------
* Fixed: reading RootNode on Prediktor.
* Added: NonFiniteReplacement config.

0.5.0  2019-09-23
------------------
* ...