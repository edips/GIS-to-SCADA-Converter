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
    kofra.SHAPE.STEndPoint().STX AS Point_X,
    kofra.SHAPE.STEndPoint().STY AS Point_Y,
    kofra.ASSETID,
    kofra.NAME AS LOAD_CURVE1,
	kofra.OPERATING_VOLTAGE as Nominal_Voltage,
    bnd.Z_TEAR_ID as TeAR
    FROM ELE_HOUSE_CONNECTION_evw kofra
    left join BNDRY_OPERATIONAL_CENTER bnd on kofra.OPERATIONALCENTER_REF = bnd.CENTER_CODE""")
    # Okunan degerleri dataframe olarak aktar:
    p_dataframe = pd.read_sql_query(sql, sqlalchemy_connection())
    return p_dataframe


def sabit_degerler(p_dataframe):
    p_dataframe['Phase_Designation'] = 7
    p_dataframe['Status_Indicator'] = 3
    p_dataframe['Facility_ID'] = ''
    p_dataframe['B3_Text'] = ''
    p_dataframe['Substation'] = ''
    p_dataframe['Conforming Load(Y / N)'] = ''
    p_dataframe['Load Behaviour Type'] = ''
    p_dataframe['Nominal Load'] = ''
    p_dataframe['MAXPERCLOAD'] = ''
    p_dataframe['No. of important customers'] = ''
    p_dataframe['POWERFACTOR'] = ''
    p_dataframe['LOAD_CURVE_PER1 %'] = ''
    p_dataframe['LOADCURV 2'] = ''
    p_dataframe['CURV2 %'] = ''
    p_dataframe['LOADCURV 3'] = ''
    p_dataframe['CURV3 %'] = ''


def final_dataframe(p_dataframe):
    p_dataframe.replace(to_replace=[r"\\t|\\n|\\r", "\t|\n|\r"], value=["", ""], regex=True, inplace=True)
    return p_dataframe[[
        'GIS_ID',
        'Status_Indicator',
        'Phase_Designation',
        'Facility_ID',
        'B3_Text',
        'Substation',
        'TeAR',
        'Nominal_Voltage',
        'Conforming Load(Y / N)',
        'Load Behaviour Type',
        'Nominal Load',
        'MAXPERCLOAD',
        'No. of important customers',
        'POWERFACTOR',
        'LOAD_CURVE1',
        'LOAD_CURVE_PER1 %',
        'LOADCURV 2',
        'CURV2 %',
        'LOADCURV 3',
        'CURV3 %'
    ]]


# dataframe'yi csv ve excel'e aktar
def dataframe_export(p_dataframe):
    # p_dataframe.to_csv(r".\Outputs\load.csv", sep=';', encoding='utf-8', index=False)
    p_dataframe.to_csv(r"..\Outputs\load.csv", sep=';', encoding='utf-8', index=False)
    print ("CSV donusumu tamamlandi-----------load.csv")
    # p_dataframe.to_excel(r".\Outputs\load.xlsx", 'Sheet1', p_dataframe.columns, index=False)
    p_dataframe.to_excel(r"..\Outputs\load.xlsx", 'Sheet1', p_dataframe.columns, index=False)
    print ("Excel donusumu tamamlandi-----------load.xlsx")


# CSV dosyasndan DXF olusturma fonksiyonu
def export_DXF():
    if not os.path.exists("..\\Outputs\\DXF"):
        os.makedirs("..\\Outputs\\DXF")
    # Parametrelerin tanimlanmasi
    if os.path.exists("output_data.gdb"):
        shutil.rmtree("output_data.gdb")
    # dxf ve geodatabase klasorunu sil
    # dxf dosyasinin silinmesinin sebebi dxf tekrar olustururken var olan dxf dosyasina eklenmesi ve dosya boyutunu sisirmesidir.
    dxf_path = "..\\Outputs\\DXF\\load.DXF"
    gdb_folder = 'output_data.gdb'
    csv_path = "..\\Outputs\\load.csv"
    point_feature_class_dir = "\\load"
    point_feature_class = "load"
    dosya_sil(dxf_path)
    # feature class'in koordinat sistemi degeri
    spatial_ref = arcpy.Describe(get_input() + '\ELE_HOUSE_CONNECTION_evw').spatialReference
    # DXF dosya donusumu: donusumu tamamlanan csv dosyasindan X ve Y koordinatlarinin DXF'ye donusumu
    # Geodatabase'de transformer feature class olusturulmasi
    # featureclass_path = '..\\output_data.gdb\\transformer'
    featureclass_path = 'output_data.gdb\\load'
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
        arcpy.MakeXYEventLayer_management(csv_path, 'Point_X', 'Point_Y', featureclass_path, spatial_ref, '#')
        arcpy.FeatureClassToFeatureClass_conversion(featureclass_path, gdb_folder, point_feature_class)
        print ("CSV tablosundan Feature Class olusturma islemi tamamlandi.")
    # 2- olusturulan feature class DXF'ye donusturulur.
    arcpy.ExportCAD_conversion(gdb_folder + point_feature_class_dir, 'DXF_R2013', dxf_path,
                               'Ignore_Filenames_in_Tables', 'Overwrite_Existing_Files', '#')
    print("DXF donusumu tamamlandi.")
    dosya_sil(dxf_path + ".xml")