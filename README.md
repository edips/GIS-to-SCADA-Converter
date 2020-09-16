# GIS-to-SCADA-Converter
I designed and developed this program for electrical distribution company(OEDAŞ)’s SCADA system at Universal.Our customers wanted to use this program to automate the conversation. This converts GIS database to Siemens's SCADA data formats. SCADA formats consist of csv and AutoCAD DXF file. After running script.py, it connects Microsoft SQL server via ArcGIS/ArcSDE. Then it connects relevant tables and fetches and process data. After processing the data, it creates folder and it exports data as csv and AutoCAD DXF format. Processing millions of records takes about 30 minutes. This project is the first project in Turkey for converting GIS database to SCADA system and it is still work in process at Universal Inc. The uncomplate work for the project is geometric network analysis.

To run the project:
(Path to ArcGIS Python folder)/python.exe script.py

Requirements:
- ArcPy/ArcGIS
- Pandas
- SQLAlchemy
- PyODBC
- numpy
- pyproj
- urllib
