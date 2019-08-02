# -*- coding: utf8 -*-
# Author : Edip Ahmet TASKIN, Universal Yazilim A.Ş.
from common import *
import pandas as pd
import arcpy
import shutil
# Veritabanindan veri cekme ve ilk dataframeyi olusturma
def dataframe_from_database():
    sql = (""" 
    SELECT
	station.SHAPE.STCentroid ( ).STX as Point_X, 
    station.SHAPE.STCentroid ( ).STY as Point_Y, 
    station.ASSETID,
    station.ASSET_NUMBER as Name,
	station.BUILDING_NAME as B1_Text,
	station.RTU_TYPE as Telemetered,
	station.CLASS as Type,
    bnd.Z_TEAR_ID as TeAR
    FROM ELE_STATION_evw station
    left join BNDRY_OPERATIONAL_CENTER bnd on station.OPERATIONALCENTER_REF = bnd.CENTER_CODE 
    """)
    # Okunan degerleri dataframe olarak aktar:
    p_dataframe = pd.read_sql_query(sql, sqlalchemy_connection())
    return p_dataframe

def sabit_degerler(p_dataframe):
    p_dataframe['Status_Indicator'] = 3
    p_dataframe['ASSETID'] = desimal_sil(p_dataframe, 'ASSETID')
    p_dataframe.loc[p_dataframe['Telemetered'].str.contains('VAR'), 'Telemetered'] = 'C'
    p_dataframe.loc[p_dataframe['Telemetered'].str.contains('YOK'), 'Telemetered'] = 'N'
    p_dataframe['GIS_ID'] = "STA" + "000000000" + p_dataframe['ASSETID']

def final_dataframe(p_dataframe):
    #
    p_dataframe.replace(to_replace=[r"\\t|\\n|\\r", "\t|\n|\r"], value=["", ""], regex=True, inplace=True)
    # NULL değerleri boş yap
    p_dataframe = p_dataframe.fillna('')
    # Dataframe'nin son hali
    return p_dataframe[[
        'Status_Indicator',
        'Name',
        'Telemetered',
        'Type',
        'GIS_ID',
        'Point_X',
        'Point_Y',
        'Xwgs84',
        'Ywgs84',
        'TeAR',
        'B1_Text'
    ]]

# CSV dosyasndan DXF olusturma fonksiyonu
def export_DXF():
    if not os.path.exists("..\\Outputs\\DXF"):
        os.makedirs("..\\Outputs\\DXF")
    # Parametrelerin tanimlanmasi

    # dxf ve geodatabase klasorunu sil
    # dxf dosyasinin silinmesinin sebebi dxf tekrar olustururken var olan dxf dosyasina eklenmesi ve dosya boyutunu sisirmesidir.
    dxf_path = "..\\Outputs\\DXF\\station.DXF"
    gdb_folder = 'output_data.gdb'
    csv_path = "..\\Outputs\\station.csv"
    point_feature_class_dir = "\\station"
    point_feature_class = "station"
    dosya_sil(dxf_path)
    if os.path.exists("output_data.gdb"):
        shutil.rmtree("output_data.gdb")
    # feature class'in koordinat sistemi degeri
    spatial_ref = arcpy.Describe(get_input() + '\ELE_STATION').spatialReference
    # DXF dosya donusumu: donusumu tamamlanan csv dosyasindan X ve Y koordinatlarinin DXF'ye donusumu
    featureclass_path = 'output_data.gdb\\station'
    # current directory
    directory_gdb = os.getcwd()
    # gdb klasoru varsa sil

    # Donusum islemi
    if os.path.exists(gdb_folder) and os.path.isdir(gdb_folder):
        dosya_sil(dxf_path)
    print("Koordinat sistemi: ", spatial_ref.Name)
    # gdb olustur
    arcpy.CreateFileGDB_management(directory_gdb, gdb_folder)
    # csv'den dxf'ye donusum islemi
    # 1- ilk olarak csv'den X ve Y koordinatlari cekilip output.gdb'de point feature class olusturulur
    if arcpy.Exists(gdb_folder + point_feature_class_dir):
        pass
    else:
        arcpy.MakeXYEventLayer_management(csv_path, 'Point_X', 'Point_Y', featureclass_path, spatial_ref,'#')
        arcpy.FeatureClassToFeatureClass_conversion(featureclass_path, gdb_folder, point_feature_class)
        print ("CSV tablosundan Feature Class olusturma islemi tamamlandi.")
    # 2- olusturulan feature class DXF'ye donusturulur.
    arcpy.ExportCAD_conversion(gdb_folder + point_feature_class_dir, 'DXF_R2013', dxf_path,
                               'Ignore_Filenames_in_Tables', 'Overwrite_Existing_Files', '#')
    print("DXF donusumu tamamlandi.")
    dosya_sil(dxf_path + ".xml")