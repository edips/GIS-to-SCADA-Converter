# -*- coding: utf8 -*-
from arcpy import mapping as mp
from arcpy import CalculateField_management as cfield
from arcpy import Describe, AddMessage as msg
from arcpy import da
import pandas as pd
import numpy as np
from lib_node import *
import gc
if not gc.isenabled():
    gc.enable()


class Node:
    def __init__(self):
        self.env.overwriteOutput = True
        self.env.workspace = r"C:\Users\Asus\Desktop\DATA\istasyon_tracking_multiple_stations\scada_network_sade.gdb"
        self.bara_path = "C:\\Users\\Asus\\Desktop\\DATA\\istasyon_tracking_multiple_stations\\scada_network_sade.gdb\\ELE\\ELE_DD_CONNECTOR"
        self.anahtar_path = "C:\\Users\\Asus\\Desktop\\DATA\\istasyon_tracking_multiple_stations\\scada_network_sade.gdb\\ELE\\ELE_DD_SEPARATOR_POINT"
        self.my_network2 = r"C:\Users\Asus\Desktop\DATA\istasyon_tracking_multiple_stations\scada_network_sade.gdb\ELE\ELE_Net"
        self.csv_path = r"C:\Oedas\ScadaEntegrasyonu\Outputs\node.csv"

        self.runforme()

    def addFields(self):
        # Add Node_ID field
        fields_bara = ['Node_ID', 'FeatureClass', 'GIS_ID', 'Terminal']
        fields_anahtar = ['Terminal1', 'Terminal2', 'Node_ID1', 'Node_ID2', 'GIS_ID']
        #
        for f in fields_bara:
            try:
                if len(arcpy.ListFields(bara_path, f)):
                    arcpy.AddField_management(bara_path, f, 'TEXT')

            except Exception, ErrorDesc:
                raise Exception('{} feature classina {} field eklerken hata aldim : {}'.
                                format(Describe(bara_path).name.encode('utf-8'), f, ErrorDesc))

        for f in fields_anahtar:
            try:
                if len(arcpy.ListFields(anahtar_path, f)):
                    arcpy.AddField_management(anahtar_path, f, 'TEXT')

            except Exception, ErrorDesc:
                raise Exception('{} feature classina {} field eklerken hata aldim : {}'.
                                format(Describe(anahtar_path).name.encode('utf-8'), f, ErrorDesc))

    # çizgideki orta noktayi bul
    @staticmethod
    def find_mid_pnt_object(point):
        pnt = arcpy.Point()
        pnt.X, pnt.Y = point.positionAlongLine(0.50, True).firstPoint.X, \
                       point.positionAlongLine(0.50, True).firstPoint.Y
        return arcpy.PointGeometry(pnt)

    # pandasta yer degistşrme
    @staticmethod
    def yerdegistir(p_dataframe, kolon, eski_deger, yeni_deger):
        return p_dataframe[kolon].replace(eski_deger, yeni_deger)

    # convert fields to list in a feature class
    @staticmethod
    def flatten(list_lists):
        return list(itertools.chain.from_iterable(list_lists))

    def create_cursors(self):
        ucursor = da.SearchCursor(self.bara_path, ["FeatureClass", "Node_ID", "SHAPE@", "ASSETID"],
                                  """Node_ID IS NULL and FeatureClass = 'Node' and subtyp=2""")
        # FeatureClass'i Bara olan objelere Node_ID degerlerinin verilmesi
        ucursor2 = da.SearchCursor(self.bara_path, ["FeatureClass", "Node_ID", "SHAPE@", "ASSETID"],
                                   """Node_ID IS NULL and FeatureClass = 'Bara' and subtyp=2""")

        return [ucursor, ucursor2]

    def node_generator_c2(self, ucursor2):
        counter = 0
        for row in ucursor2:
            shp = row[2]
            pnt_geom = find_mid_pnt_object(shp)
            arcpy.TraceGeometricNetwork_management(self.my_network2, "", pnt_geom,
                                                   "FIND_CONNECTED", "", "", "", "",
                                                   ["ELE_DD_SEPARATOR_POINT", "ELE_CABLE",
                                                    "ELE_DD_TRANSFORMER"])
            mxd = mp.MapDocument("CURRENT")
            df = mp.ListDataFrames(mxd)[0]  # which means layers dataframe
            df.zoomToSelectedFeatures()
            all_layers = mp.ListLayers(mxd, "*", df)
            bara_name = Describe(self.bara_path).name.encode('utf-8')

            if counter != 0:
                bara_layer = "%s_%s\%s" % (Describe(self.my_network2).name.encode('utf-8'), counter, bara_name)
            else:
                bara_layer = "%s\%s" % (Describe(self.my_network2).name.encode('utf-8'), bara_name)

            newlyrname = 'bara_%s' % counter
            newlyrname2 = 'anahtar_%s' % counter

            arcpy.MakeFeatureLayer_management(bara_layer, newlyrname)
            arcpy.MakeFeatureLayer_management(anahtar_path, newlyrname2)

            arcpy.SelectLayerByLocation_management(newlyrname2, 'BOUNDARY_TOUCHES', newlyrname, '#', 'NEW_SELECTION',
                                                   'NOT_INVERT')
            min_assetid = str(min(flatten([r for r in row2] for row2 in arcpy.da.SearchCursor(newlyrname, "ASSETID"))))

            node_id = 'NODE' + '00000000' + min_assetid

            gis_id = 'Busbar' + '000000' + min_assetid

            cfield(newlyrname, "GIS_ID", '"%s"' % gis_id, 'PYTHON_9.3')
            cfield(newlyrname, 'Node_ID', '"%s"' % node_id, 'PYTHON_9.3')
            cfield(newlyrname, "Terminal", '"N"', "PYTHON")
            # anahtarlama katmanı için Baraya komşu olan anahtarların terminali bir olur.
            cfield(newlyrname2, "Terminal1", '"1"', "PYTHON")
            cfield(newlyrname2, "Node_ID1", '"1"', "PYTHON")

            try:
                [mp.RemoveLayer(df, lyr) for lyr in all_layers]
            except:
                msg("Layer silme isini maalesef yapamadim")
            counter += 1

    def node_generator_c1(self, ucursor1):
        counter = 0

        for row in ucursor1:
            shp = row[2]
            pnt_geom = find_mid_pnt_object(shp)
            arcpy.TraceGeometricNetwork_management(self.my_network2, "", pnt_geom,
                                                   "FIND_CONNECTED", "", "", "", "",
                                                   ["ELE_DD_SEPARATOR_POINT", "ELE_CABLE",
                                                   "ELE_DD_TRANSFORMER"])

            mxd = mp.MapDocument("CURRENT")
            df = mp.ListDataFrames(mxd)[0]  # which means layers dataframe
            df.zoomToSelectedFeatures()
            all_layers = mp.ListLayers(mxd, "*", df)
            bara_name = Describe(self.bara_path).name.encode('utf-8')

            if counter != 0:
                bara_layer = "%s_%s\%s" % (Describe(self.my_network2).name.encode('utf-8'), counter, bara_name)
            else:
                bara_layer = "%s\%s" % (Describe(self.my_network2).name.encode('utf-8'), bara_name)

            newlyrname = 'bara_%s' % counter
            arcpy.MakeFeatureLayer_management(bara_layer, newlyrname)

            node_id = 'NODE' + '00000000' + str(row[3])
            cfield(newlyrname, 'Node_ID', '"%s"' % node_id, 'PYTHON_9.3')

            try:
                [mp.RemoveLayer(df, lyr) for lyr in all_layers]
            except:
                msg("Layer silmede hata.")
            counter += 1

    def export_to_csv(self, csv_path):
        # Node_ID kolonlarinin csv'ye aktarimi
        npArray = arcpy.da.FeatureClassToNumPyArray(self.bara_path, ["Node_ID"], null_value=None)
        # nplist = [i.replace(str(-9999), '') for i in list(npArray['Node_ID'])]
        df = pd.DataFrame(npArray, columns=['Node_ID'])
        df = df.replace(['nan', 'None'], '')
        df['Node_ID'].replace('', np.nan, inplace=True)
        # drop all NULL values
        df.dropna(subset=['Node_ID'], inplace=True)
        print df

        if not csv_path:
            csv_path = r"C:\Oedas\ScadaEntegrasyonu\Outputs\node.csv"

        df.to_csv(csv_path, sep=';', encoding='utf-8', na_rep=" ", index=False, )
        print "csv'ye donusum tamamlandi."

    def runforme(self):
        # FeatureClass'i Node olan objelere Node_ID degerlerinin verilmesi
        ucursor = da.SearchCursor(bara_path, ["FeatureClass", "Node_ID", "SHAPE@", "ASSETID"],
                                  """Node_ID IS NULL and FeatureClass = 'Node' and subtyp=2""")
        # FeatureClass'i Bara olan objelere Node_ID degerlerinin verilmesi
        ucursor2 = da.SearchCursor(bara_path, ["FeatureClass", "Node_ID", "SHAPE@", "ASSETID"],
                                   """Node_ID IS NULL and FeatureClass = 'Bara' and subtyp=2""")

        self.addFields()
        self.node_generator_c2(ucursor2)
        self.node_generator_c2(ucursor)

