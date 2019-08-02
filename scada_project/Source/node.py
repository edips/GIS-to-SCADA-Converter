# -*- coding: utf8 -*-
from arcpy import mapping as mp
from arcpy import CalculateField_management as cfield
from arcpy import Describe, AddMessage as msg
from arcpy import da
import pandas as pd
import numpy as np
from arcpy import env
import itertools
import arcpy
import gc
if not gc.isenabled():
    gc.enable()

## lib_node.py ##
# paths
env.overwriteOutput = True
env.workspace = r"C:\Users\Asus\Desktop\DATA\istasyon_tracking_multiple_stations\scada_network_sade.gdb"
dataset="C:\\Users\\Asus\\Desktop\\DATA\\istasyon_tracking_multiple_stations\\scada_network_sade.gdb\\ELE"
bara_path = "C:\\Users\\Asus\\Desktop\\DATA\\istasyon_tracking_multiple_stations\\scada_network_sade.gdb\\ELE\\ELE_DD_CONNECTOR"
anahtar_path = "C:\\Users\\Asus\\Desktop\\DATA\\istasyon_tracking_multiple_stations\\scada_network_sade.gdb\\ELE\\ELE_DD_SEPARATOR_POINT"
anahtar_path2 = r'C:\Users\Asus\Desktop\DATA\istasyon_tracking_multiple_stations\scada_network_sade.gdb\ELE\switch'
bara_path2 = r'C:\Users\Asus\Desktop\DATA\istasyon_tracking_multiple_stations\scada_network_sade.gdb\ELE\bara'
my_network2 = r"C:\Users\Asus\Desktop\DATA\istasyon_tracking_multiple_stations\scada_network_sade.gdb\ELE\ELE_Net"

# ASSETID degerlerinin bara için BB_ASSETID ve anahtar için SW_ASSETID olarak verilmesi
if len(arcpy.ListFields(anahtar_path, "SW_ASSETID")) > 0:
    print "%s field already exists." % "SW_ASSETID"
    pass
else:
    arcpy.AlterField_management(anahtar_path, "ASSETID", 'SW_ASSETID', 'SW_ASSETID')
if len(arcpy.ListFields(bara_path, "BB_ASSETID")) > 0:
    print "%s field already exists." % "BB_ASSETID"
    pass
else:
    arcpy.AlterField_management(bara_path, "ASSETID", 'BB_ASSETID', 'BB_ASSETID')

# bara ve anahtar katmanlarına alan ekleme:
# bara için 'Node_ID', 'BB_GIS_ID', 'BB_Terminal'
# anahtar için 'SW_Terminal', 'SW_GIS_ID' alanları eklenir.
def addFields():
    print "function is running.."
    # Add Node_ID field
    fields_bara = ['Node_ID', 'BB_GIS_ID', 'BB_Terminal']
    fields_anahtar = ['SW_Terminal', 'SW_GIS_ID']

    for f in fields_bara:
        try:
            if len(arcpy.ListFields(bara_path, f)) > 0:
                print "%s field already exists." % f
                pass
            else:
                arcpy.AddField_management(bara_path, f, "TEXT")
                print "%s field has been created." % f
        except Exception, ErrorDesc:
            raise Exception('{} feature classina {} field eklerken hata aldim : {}'.
                            format(Describe(bara_path).name.encode('utf-8'), f, ErrorDesc))

    for f in fields_anahtar:
            try:
                if len(arcpy.ListFields(anahtar_path, f)) > 0:
                    print "%s field already exists." % f
                    pass
                else:
                    arcpy.AddField_management(anahtar_path, f, "TEXT")
                    print "%s field has been created." % f
            except Exception, ErrorDesc:
                raise Exception('{} feature classina {} field eklerken hata aldim : {}'.
                                format(Describe(anahtar_path).name.encode('utf-8'), f, ErrorDesc))

# Çizgideki orta noktayi bulma
def find_mid_pnt_object(point):
    pnt = arcpy.Point()
    pnt.X, pnt.Y = point.positionAlongLine(0.50, True).firstPoint.X, \
                   point.positionAlongLine(0.50, True).firstPoint.Y
    return arcpy.PointGeometry(pnt)

# convert fields to list in a feature class
def feature_to_list(list_lists):
    return list(itertools.chain.from_iterable(list_lists))

# add necessary fields
addFields()

counter = 0
# FeatureClass'i Node olan objelere Node_ID degerlerinin verilmesi
ucursor = da.SearchCursor(bara_path, ["Node_ID", "SHAPE@", "BB_ASSETID"],
                          """Node_ID IS NULL and Facility_ID is NULL and subtyp=2""")
# FeatureClass'i Bara olan objelere Node_ID degerlerinin verilmesi
ucursor2 = da.SearchCursor(bara_path, ["Node_ID", "SHAPE@", "BB_ASSETID"],
                          """Node_ID IS NULL and Facility_ID is not NULL and subtyp=2""")

########################## NODE_ID GENERATION (NODE and BUSBAR) ############################################################
for row in ucursor2:
    # Node_ID
    nid = row[0]
    # SHAPE@
    shp = row[1]
    # Bara çizgisinin orta noktası
    pnt_geom = find_mid_pnt_object(shp)
    # Herhangi bir baranın ortasından atılan nokta flag olarak alınır ve trace işlemi "FIND CONNECTED" olarak yapılır.
    trace_group_layers = arcpy.TraceGeometricNetwork_management(my_network2, "", pnt_geom,
                                                                "FIND_CONNECTED", "", "", "", "",
                                                                ["ELE_DD_SEPARATOR_POINT", "ELE_CABLE", "ELE_DD_TRANSFORMER"])
    mxd = mp.MapDocument("CURRENT")
    df = mp.ListDataFrames(mxd)[0]  # which means layers dataframe
    df.zoomToSelectedFeatures()
    all_layers = mp.ListLayers(mxd, "*", df)
    bara_name = Describe(bara_path).name.encode('utf-8')
    anahtar_name = Describe(anahtar_path).name.encode('utf-8')
    if counter != 0:
        bara_layer = "%s_%s\%s" % (Describe(my_network2).name.encode('utf-8'), counter, bara_name)
    else:
        bara_layer = "%s\%s" % (Describe(my_network2).name.encode('utf-8'), bara_name)

    newlyrname = 'bara_%s' % counter
    arcpy.MakeFeatureLayer_management(bara_layer, newlyrname)

    # Trace sonucu seçilen bara grubundan ASSETID'si en küçük olan seçilir ve bara grubunun tamamına bu ASSETID yazılır.
    min_assetid = str(min(feature_to_list([r for r in row2] for row2 in arcpy.da.SearchCursor(newlyrname, "BB_ASSETID"))))

    # Analiz dökümanına göre NODE_ID ve GIS_ID'nin isimlendirilmesi
    node_id = 'NODE' + '00000000' + min_assetid
    gis_id = 'Busbar' + '000000' + min_assetid
    # SCADA'daki baranın GIS_ID, Node_ID ve Terminal kolonlarının aktarımı.
    cfield(newlyrname, "BB_GIS_ID", '"%s"' % gis_id, 'PYTHON_9.3')
    cfield(newlyrname, 'Node_ID', '"%s"' % node_id, 'PYTHON_9.3')
    cfield(newlyrname, "BB_Terminal", '"N"', "PYTHON")

    try:
        [mp.RemoveLayer(df, lyr) for lyr in all_layers]
    except:
        msg("Unable to delete the layer.")
    counter += 1
del ucursor2

########################## NODE_ID GENERATION (NODE ONLY) ############################################################
for row in ucursor:
    nid = row[0]
    shp = row[1]
    pnt_geom = find_mid_pnt_object(shp)
    trace_group_layers = arcpy.TraceGeometricNetwork_management(my_network2, "", pnt_geom,
                                                        "FIND_CONNECTED", "", "", "", "",
                                                        ["ELE_DD_SEPARATOR_POINT", "ELE_CABLE", "ELE_DD_TRANSFORMER"])

    mxd = mp.MapDocument("CURRENT")
    df = mp.ListDataFrames(mxd)[0]  # which means layers dataframe
    df.zoomToSelectedFeatures()
    all_layers = mp.ListLayers(mxd, "*", df)
    bara_name = Describe(bara_path).name.encode('utf-8')
    if counter != 0:
        bara_layer = "%s_%s\%s" % (Describe(my_network2).name.encode('utf-8'), counter, bara_name)
    else:
        bara_layer = "%s\%s" % (Describe(my_network2).name.encode('utf-8'), bara_name)

    newlyrname = 'bara_%s' % counter
    arcpy.MakeFeatureLayer_management(bara_layer, newlyrname)

    node_id = 'NODE'+'00000000' + str(row[2])
    cfield(newlyrname, 'Node_ID', '"%s"' % node_id, 'PYTHON_9.3')

    try:
        [mp.RemoveLayer(df, lyr) for lyr in all_layers]
    except:
        msg("Unable to delete the layer.")
    counter += 1
del ucursor

print "Baralara Node_ID ve GIS_ID atama islemi bitti."

# Facility_ID'si olan baralara komşu olan tüm anahtarların Terminal kolonuna 1 değerinin verilmesi
# once Facility_ID'si olan tüm baralar seçilir


arcpy.arcpy.MakeFeatureLayer_management(bara_path, "bara_selected")
arcpy.SelectLayerByAttribute_management("bara_selected", 'NEW_SELECTION', 'Facility_ID is not NULL')
# select layer by location ile baraya komşu anahtarlar seçilir.
arcpy.arcpy.MakeFeatureLayer_management(anahtar_path, "anahtar_selected")
arcpy.SelectLayerByLocation_management("anahtar_selected", "INTERSECT", "bara_selected", "2 Centimeters",
                                       "NEW_SELECTION", "NOT_INVERT")
# seçili anahtarların Terminal alanına 1 değerinin verilmesi
cfield("anahtar_selected", 'SW_Terminal', '"1"', 'PYTHON_9.3')
arcpy.SelectLayerByAttribute_management("anahtar_selected", "CLEAR_SELECTION")
arcpy.SelectLayerByAttribute_management("bara_selected", "CLEAR_SELECTION")



print "Facility_ID'si olan Baralara komsu olan Anahtarların Terminaline 1 degeri verildi."

# Anahtarlarin GIS_ID'sine deger verilemsi

sw_gis_id = 'SWITCH' + '000000' + "!" + str("SW_ASSETID") + "!"
cfield(anahtar_path, 'SW_GIS_ID', '"%s"' % sw_gis_id, 'PYTHON_9.3')
print "Anahtarlara GIS_ID degerleri verildi."


# selecting necessary feature class
if arcpy.Exists(anahtar_path2) or arcpy.Exists(bara_path2):
    pass
else:
    arcpy.Select_analysis(anahtar_path, anahtar_path2, 'SUBTYP in (4,5,6)')
    arcpy.Select_analysis(bara_path, bara_path2, 'SUBTYP = 2')

# bara ve anahtarlama elemanlarının spatial join işlemi
# TODO; GDB path should be parameterized with %s
arcpy.SpatialJoin_analysis(r'C:\Users\Asus\Desktop\DATA\istasyon_tracking_multiple_stations\scada_network_sade.gdb\ELE\switch',
                           r'C:\Users\Asus\Desktop\DATA\istasyon_tracking_multiple_stations\scada_network_sade.gdb\ELE\bara',
                           r'C:\Users\Asus\Documents\ArcGIS\Default.gdb\switch_bara_joined',
                           'JOIN_ONE_TO_MANY',
                           'KEEP_ALL',
                           r'SUBTYP "Alttip" true true false 4 Long 0 0 ,First,#,C:\Users\Asus\Desktop\DATA\istasyon_tracking_multipl'
                           r'e_stations\scada_network_sade.gdb\ELE\switch,SUBTYP,-1,-1;SUBTYP_1 "Alttip" true true false 4 Long 0 0 ,'
                           r'First,#,C:\Users\Asus\Desktop\DATA\istasyon_tracking_multiple_stations\scada_network_sade.gdb\ELE\bara,SUB'
                           r'TYP,-1,-1;SW_ASSETID "SW_ASSETID" true true false 4 Long 0 0 ,First,#,C:\Users\Asus\Desktop\DATA\istasyon_tracki'
                           r'ng_multiple_stations\scada_network_sade.gdb\ELE\switch,SW_ASSETID,-1,-1;BB_ASSETID "BB_ASSETID" true true false 4'
                           r' Long 0 0 ,First,#,C:\Users\Asus\Desktop\DATA\istasyon_tracking_multiple_stations\scada_network_sade.gdb\ELE\bara,'
                           r'BB_ASSETID,-1,-1;Facility_ID "Facility_ID" true true false 255 Text 0 0 ,First,#,C:\Users\Asus\Desktop\DATA\istasyon'
                           r'_tracking_multiple_stations\scada_network_sade.gdb\ELE\bara,Facility_ID,-1,-1;Node_ID "Node_ID" true true false 255 '
                           r'Text 0 0 ,First,#,C:\Users\Asus\Desktop\DATA\istasyon_tracking_multiple_stations\scada_network_sade.gdb\ELE\bara,No'
                           r'de_ID,-1,-1;SW_GIS_ID "SW_GIS_ID" true true false 255 Text 0 0 ,First,#,C:\Users\Asus\Desktop\DATA\istasyon_tracking'
                           r'_multiple_stations\scada_network_sade.gdb\ELE\switch,SW_GIS_ID,-1,-1;BB_GIS_ID "BB_GIS_ID" true true false 255 Text '
                           r'0 0 ,First,#,C:\Users\Asus\Desktop\DATA\istasyon_tracking_multiple_stations\scada_network_sade.gdb\ELE\bara,BB_GIS_ID,'
                           r'-1,-1;SW_Terminal "SW_Terminal" true true false 255 Text 0 0 ,First,#,C:\Users\Asus\Desktop\DATA\istasyon_tracking_mul'
                           r'tiple_stations\scada_network_sade.gdb\ELE\switch,SW_Terminal,-1,-1;BB_Terminal "BB_Terminal" true true false 255 Text '
                           r'0 0 ,First,#,C:\Users\Asus\Desktop\DATA\istasyon_tracking_multiple_stations\scada_network_sade.gdb\ELE\bara,BB_Termina'
                           r'l,-1,-1', 'INTERSECT', '#', '#')

# Connectivity dataframe olusturma
# anahtar - bara spatial join ile elde edilen tablodan dataframe olusturma
npArray_sw = arcpy.da.FeatureClassToNumPyArray(r'C:\Users\Asus\Documents\ArcGIS\Default.gdb\switch_bara_joined',
                                               ["SW_Terminal", "BB_Terminal", "SW_ASSETID", "Node_ID"], null_value="")
df_sw_bb = pd.DataFrame(npArray_sw)
print df_sw_bb
# TODO; spatial join ile elde edilmiş olan switch_bara_joined katmanının anahtarlama
# TODO; elemanlarının Terminaline değer atamak (1,2). Barada Terminal_ID N ise anahtarlamanın
# TODO; terminalinin 1 olması gerekir.

# TODO; Anahtarlamada aynı ASSETID'ye sahip olan kayıtlardan biri 1 ise diğeri 2 olmalı

# TODO; Anahtarlamada aynı ASSETID'ye sahip olan kayıtlardan 2side NULL ise ardışık olarak 1 ve 2 değeri basılır.

# trafo - bara spatial join ile elde edilen tablodan dataframe olusturma
# TODO; anahtar - bara'daki spatial join gibi trafo - bara katmanları arasında spatial join yapılacak.

print "bara ve anahtarlama elemanlarina join yapildi."
# Node_ID kolonlarinin csv'ye aktarimi
# Node_ID alanının dataframe'ye aktarımı
npArray = arcpy.da.FeatureClassToNumPyArray(bara_path, ["Node_ID"], null_value=None)
df = pd.DataFrame(npArray, columns=['Node_ID'])
# nan değerler yerine boşluk atanması
df = df.replace(['nan', 'None'], '')
df['Node_ID'].replace('', np.nan, inplace=True)
# drop all NULL values
df.dropna(subset=['Node_ID'], inplace=True)
# node.csv'nin export edilmesi
csv_path = r"C:\Oedas\ScadaEntegrasyonu\Outputs\node.csv"
df.to_csv(csv_path, sep=';', encoding='utf-8', na_rep=" ", index=False,)
print "csv'ye donusum tamamlandi."