# -*- coding: utf8 -*-
# Author : Edip Ahmet TASKIN, Universal Yazilim A.Ş.
import arcpy
from site import addsitedir
from sys import executable
from os import path
import shutil
from common import *
import pandas as pd
import numpy as np

interpreter = executable
sitepkg = path.dirname(interpreter) + "\\site-packages"
addsitedir(sitepkg)

# Veritabanindan veri cekme ve ilk dataframeyi olusturma
def dataframe_from_database():
    sql = """\
    select 
    generator.ASSETID, 
    generator.SHAPE.STX as Point_X,
    generator.SHAPE.STY as Point_Y,
    istasyon.ASSETID as Substation,
    generator.GENERATOR_BRANDNAME as B3_Text,
    GENERATOR_TYP as Gen_Type,
    bnd.Z_TEAR_ID as TeAR
    FROM ELE_GENERATOR_evw generator
    left join BNDRY_OPERATIONAL_CENTER bnd on generator.OPERATIONALCENTER_REF = bnd.CENTER_CODE
    left JOIN ELE_STATION_evw istasyon ON generator.Shape.STIntersects(istasyon.Shape) = 1
    """
    # Okunan degerleri dataframe olarak aktar:
    p_dataframe = pd.read_sql_query(sql, sqlalchemy_connection())
    return p_dataframe

# Facility_ID kolonu istasyonun icinde kumulatif olarak toplama yontemine gore siralanir
def facility_id_query(p_dataframe):
    p_dataframe['Facility_ID'] = p_dataframe.groupby('Substation').cumcount() + 1
    p_dataframe.loc[p_dataframe['Substation'].isnull(), 'Facility_ID'] = p_dataframe['Substation']
    return p_dataframe["Facility_ID"]

# sabit degerlerin atanmasi:
def dataframeye_sabit_degerleri_ata(p_dataframe):
    p_dataframe['GIS_ID'] = "GEN000000000" + p_dataframe['ASSETID'].astype(str)
    p_dataframe['Status_Indicator'] = "3"
    p_dataframe['Phase_Designation'] = "7"
    p_dataframe['Nominal_Voltage'] = ""
    p_dataframe['Min Generation kW'] = ""
    p_dataframe['Max Generation kW'] = ""
    p_dataframe['Min Generation kVAr'] = ""
    p_dataframe['Max Generation kVAr'] = ""
    p_dataframe['Resist_Gen'] = ""
    p_dataframe['React_Gen'] = ""
    p_dataframe['Resist_Pos'] = ""
    p_dataframe['React_Pos'] = ""
    p_dataframe['Resist_Zero'] = ""
    p_dataframe['React_Zero'] = ""

# kolonlarin siralanmasi ve dataframe'nin aktarima hazir hali, dataframenin son halinin csv'ye export edilmesi
def final_dataframe(p_dataframe):
    # entera basilmis gibi gelen bos yerlerin duzeltilmesi islemi
    p_dataframe.replace(to_replace=[r"\\t|\\n|\\r", "\t|\n|\r"], value=["", ""], regex=True, inplace= True)
    # Ondalik kisimi olmamasi gereken serilerin ondalik degerlerinin silinmesi:
    p_dataframe["Facility_ID"] = p_dataframe['Facility_ID'].astype(str).replace('\.0', '', regex=True)
    p_dataframe["Substation"] = p_dataframe['Substation'].astype(str).replace('\.0', '', regex=True)
    p_dataframe.loc[p_dataframe['Substation'].str.contains('nan'), 'Facility_ID'] = ''
    p_dataframe.loc[p_dataframe['Substation'].str.contains('nan'), 'Substation'] = ''
    p_dataframe = p_dataframe.fillna('')

    return p_dataframe[[
        'GIS_ID',
        'Status_Indicator',
        'Phase_Designation',
        'B3_Text',
        'Facility_ID',
        'Substation',
        'TeAR',
        'Nominal_Voltage',
       'Gen_Type',
        'Min Generation kW',
        'Max Generation kW',
        'Min Generation kVAr',
        'Max Generation kVAr',
        'Resist_Gen',
        'React_Gen',
        'Resist_Pos',
        'React_Pos',
        'Resist_Zero',
        'React_Zero',
        'Point_X',
        'Point_Y',
        "Xwgs84",
        "Ywgs84" ]]

# CSV dosyasndan DXF olusturma fonksiyonu
def export_DXF():
    if not os.path.exists("..\\Outputs\\DXF"):
        os.makedirs("..\\Outputs\\DXF")
    # Parametrelerin tanimlanmasi
    if os.path.exists("output_data.gdb"):
        shutil.rmtree("output_data.gdb")
    # dxf ve geodatabase klasorunu sil
    # dxf dosyasinin silinmesinin sebebi dxf tekrar olustururken var olan dxf dosyasina eklenmesi ve dosya boyutunu sisirmesidir.
    dxf_path = "..\\Outputs\\DXF\\generator.DXF"
    gdb_folder = 'output_data.gdb'
    csv_path = "..\\Outputs\\generator.csv"
    point_feature_class_dir = "\\generator"
    point_feature_class = "generator"
    dosya_sil(dxf_path)
    # feature class'in koordinat sistemi degeri
    spatial_ref = arcpy.Describe(get_input() + '\ELE_GENERATOR').spatialReference
    # DXF dosya donusumu: donusumu tamamlanan csv dosyasindan X ve Y koordinatlarinin DXF'ye donusumu
    # Geodatabase'de transformer feature class olusturulmasi
    # featureclass_path = '..\\output_data.gdb\\transformer'
    featureclass_path = 'output_data.gdb\\generator'
    # current directory
    directory_gdb = os.getcwd()
    # gdb klasoru varsa sil
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