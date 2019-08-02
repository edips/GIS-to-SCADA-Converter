# -*- coding: utf8 -*-
# Author : Edip Ahmet TASKIN, Universal Yazilim A.Ş.
from common import *
import pandas as pd
import shutil
import arcpy

# # YENILIKLER:
# NORMAL STATE kolonu veritabanında SWITCHING_STATUS alanını referans almaktadır.
# NormalState'de AÇIK için 1, KAPALI için 0, BİLİNMEYEN için boşluk bırakılarak script güncellendi.
# Ama NormalState kolonunda çok NULL değerleri mevcut.
# AmpSC_ kolonuna 16000 değeri verildi.
# Bay Number kolonu araştırılacak. Kolonun nasıl doldurulacağına dair OEDAŞ'tan bilgi bekleniyor.
# Substation kolonu İstasyonun ASSETID'si olarak verildiğinden karakter sayısı 11'di,
# Fakat Scada 8 karakter istiyor. OEDAŞ ASSET_NUMBER
# kullanılabilir dedi ama bu kolonda NULL, string gibi değerler mevcut olup karakter sayısı değişmektedir.
# OEDAŞ'tan bu durum ile ilgili bilgi bekleniyor.
# Unique_Key OEDAŞ tarafından doldurulacak.
# Subtyp için sorguda filtreleme uygulandı:  SUBTYP in (4,5,6)
# ____________________________________________________________________
# Veritabanindan veri cekme ve ilk dataframeyi olusturma
def dataframe_from_database():
    sql = (""" 
    SELECT
	hucre.ASSET_NUMBER as Bay_number,
    anahtar.SUBTYP,
	anahtar.SHAPE.STX as Point_X, 
    anahtar.SHAPE.STY as Point_Y, 
    anahtar.ASSETID,
    anahtar.NAME_NUMBER,
    anahtar.CLASS,
	anahtar.SWITCHING_STATUS,
	anahtar.BAY_REF,
	anahtar.SHORT_CIRCUIT_CURRENT,
	anahtar.STATION_REF,
	anahtar.NOMINAL_VOLTAGE,
    anahtar.OPERATIONALCENTER_REF,
    anahtar.UNIQUE_KEY,
    bnd.Z_TEAR_ID as TeAR
    FROM ELE_DD_SEPARATOR_POINT_evw anahtar
    left join ELE_DD_BAY_evw hucre on hucre.ASSETID=anahtar.BAY_REF
    left join BNDRY_OPERATIONAL_CENTER bnd on anahtar.OPERATIONALCENTER_REF = bnd.CENTER_CODE
	where SUBTYP in (4,5,6)
    """)
    # Okunan degerleri dataframe olarak aktar:
    p_dataframe = pd.read_sql_query(sql, sqlalchemy_connection())
    return p_dataframe

# dataframe kolonlarini yeniden adlandir
def rename_dataframe(p_dataframe):
    return p_dataframe.rename(columns={
        'SWITCHING_STATUS': 'NormalState',
        'STATION_REF': 'Substation',
        'NAME_NUMBER': 'B3_Text',
        'ASSET_NUMBER': 'GIS_ID',
        'NOMINAL_VOLTAGE': 'Nominal_Voltage',
        'UNIQUE_KEY':'Unique_key'
    }, inplace=True)

# Aşağıdaki fonksiyonlar Bay_Number kolonu için Raşit beyin mantığı ile yazılmıştı,
# fakat süleyman bey bunu kabul etmeyip Bay_Number kolonunun ASSET_NUMBER kolonunu referans
#  alması gerektiğini mailde belirtti. İleride kararın değişmesi durumunda aşağıdaki fonksiyonlar kullanılabilir.
# Bu fonksiyonlar bir hücrede kaç tane anahtar olduğunu ardışık olarak numaralandırır.

#  def group_bayref():
#      sql = ("""
#      select station_ref as substation2, assetid as bayref2 from ele_dd_bay_evw
#      """)
#      # Okunan degerleri dataframe olarak aktar:
#      df_bayref = pd.read_sql_query(sql, sqlalchemy_connection())
#      return df_bayref
# def baynumber_query(p_dataframe):
#     # veritabanindaki hucre tablosundan hucre assetidsi ve istasyon idsini dataframeye aktar
#     df_bayref = group_bayref()
#     # numara tipi kolonlari stringe donustur ve nullara bos deger ata
#     p_dataframe["Bay_number"] = number2string_deleteDecimal(p_dataframe,"Bay_number")
#     df_bayref["bayref2"] = number2string_deleteDecimal(df_bayref,"bayref2")
#     df_bayref["substation2"]=number2string_deleteDecimal(df_bayref,"substation2")
#     # istasyonda bulunan hucre sayisini kumulatif olarak hesapla ve hucre icin numaralandirma islemi yap
#     df_bayref['Bay_number2_count'] = df_bayref.groupby('substation2').cumcount() + 1
#     # # numara tipi kolonlari stringe donustur ve nullara bos deger ata
#     df_bayref["Bay_number2_count"] = number2string_deleteDecimal(df_bayref,"Bay_number2_count")
#     # veritabanindaki hucre tablosunda station ref'i null olanlar icin Bay_number degeri
#     df_bayref.Bay_number2_count[df_bayref.substation2 == ""] = df_bayref.substation2
#     print df_bayref
#     print p_dataframe["Bay_number"]
#     p_dataframe["Bay_number"] = join_column_from_dataframe_to_dataframe(p_dataframe,df_bayref,"Bay_number","bayref2","Bay_number2_count")
#     p_dataframe["Bay_number2"] = p_dataframe["Bay_number"]
#     p_dataframe["Bay_number"] = "H0" + p_dataframe["Bay_number"]
#     p_dataframe.Bay_number[p_dataframe.Bay_number2 == ""] = p_dataframe.Bay_number2
#     df_bayref.Bay_number2_count[df_bayref.substation2 == ""] = df_bayref.substation2
#     print p_dataframe["Bay_number"]

# Değeri CCODE olan alanların lookup tablosundan description değerlerini çekmesi
def gis_to_scada(p_dataframe):
    gis_description_to_scada(p_dataframe, "ELE_DD_SEPARATOR_POINT_evw", 'NOMINAL_VOLTAGE', 'Nominal_Voltage')
    # gis_description_to_scada(p_dataframe, "ELE_DD_SEPARATOR_POINT_evw", 'SHORT_CIRCUIT_CURRENT', 'AmpSC_')
    gis_description_to_scada(p_dataframe, "ELE_DD_SEPARATOR_POINT_evw", 'SWITCHING_STATUS', 'NormalState')

def GIS_ID(p_dataframe):
    p_dataframe['ASSETID'] = desimal_sil(p_dataframe, 'ASSETID')
    return "LINE00000000" + p_dataframe['ASSETID'].astype(str)

def sabit_degerler(p_dataframe):
    p_dataframe['Phase_Designation'] = '7'
    p_dataframe['Status_Indicator'] = "3"
    p_dataframe['Switch_to_ground'] = "N"
    p_dataframe['AmpSC_'] = '16000'
    # GIS_ID kolonu
    p_dataframe['GIS_ID'] = "switch" + "000000" + p_dataframe['ASSETID'].astype(str)
    p_dataframe['GIS_ID'] = desimal_sil(p_dataframe,'GIS_ID')
    p_dataframe['ASSETID'] = p_dataframe['ASSETID'].astype(str)
    p_dataframe['ASSETID'] = desimal_sil(p_dataframe,'ASSETID')
    # NormalState kolonu
    p_dataframe['NormalState'] = p_dataframe['NormalState'].astype(str)
    # Analiz dökümanına göre NormalState AÇIK ise 1, KAPALI ise 0 olmalı, BİLİNMEYEN ise boş olur
    p_dataframe.loc[p_dataframe['NormalState'].str.contains('AÇIK'), 'NormalState'] = '1'
    p_dataframe.loc[p_dataframe['NormalState'].str.contains('KAPALI'), 'NormalState'] = '0'
    p_dataframe.loc[p_dataframe['NormalState'].str.contains('BİLİNMEYEN'), 'NormalState'] = ''
    # TODO: Fonksiyona donusturulecek
    # nan değerlerin boş olması gerekir.
    p_dataframe.loc[p_dataframe['ASSETID'].str.contains('nan'), 'ASSETID'] = ''

    p_dataframe['SUBTYP'] = p_dataframe['SUBTYP'].astype(str)
    p_dataframe.loc[p_dataframe['SUBTYP'].str.contains('nan'), 'SUBTYP'] = ''
    p_dataframe["SUBTYP"] = desimal_sil(p_dataframe, 'SUBTYP')
    # Subtype 4 ise Y olur.
    p_dataframe.loc[p_dataframe['SUBTYP'].str.contains('4'), 'Switch_to_ground'] = "Y"
    # Subtyp 4 ise GIS_ID DISC ile başlar.
    # Subtype 6 ise GIS_ID CB ile başlar.
    p_dataframe.loc[p_dataframe['SUBTYP'].str.contains('4'), 'GIS_ID'] = 'DISC' + "00000000" + p_dataframe['ASSETID'].astype(str)
    p_dataframe.loc[p_dataframe['SUBTYP'].str.contains('6'), 'GIS_ID'] = 'CB' + "0000000000" + p_dataframe['ASSETID'].astype(str)
    p_dataframe.GIS_ID[p_dataframe.ASSETID == ""] = p_dataframe.ASSETID

# Type kolonunda subtyp'i 4 olan DISC olur, 6 olan CB olur.
def type_kolonu(p_dataframe):
    p_dataframe['SUBTYP'] = p_dataframe['SUBTYP'].astype(str)
    p_dataframe.loc[p_dataframe['SUBTYP'].str.contains('nan'), 'SUBTYP'] = ''
    p_dataframe["SUBTYP"] = desimal_sil(p_dataframe, 'SUBTYP')
    p_dataframe['Type'] = "nan"
    p_dataframe.loc[p_dataframe['SUBTYP'].str.contains('4'), 'Type'] = 'DISC'
    p_dataframe.loc[p_dataframe['SUBTYP'].str.contains('6'), 'Type'] = 'CB'
    p_dataframe.loc[p_dataframe['Type'].str.contains('nan'), 'Type'] = ''
    return p_dataframe['Type']


# Kolonlarin siralanmasi ve dataframenin son halinin csv'ye export edilmesi
def final_dataframe(p_dataframe):
    # B3_Text'te bazı veriler nedense enter'e basılmış gönürüyor, bude csv'ye aktarırken veri kaymasına sebep oluyor.
    # Bu hatanın giderilmesi için aşağıdaki kod uygulanmıştır.
    p_dataframe.replace(to_replace=[r"\\t|\\n|\\r", "\t|\n|\r"], value=["", ""], regex=True, inplace=True)
    # Type kolonu
    p_dataframe["Type"] = type_kolonu(p_dataframe)
    # integer olan olonların decimal (.0) değerlerinin silinmesi
    p_dataframe['Unique_key'] = number2string_deleteDecimal(p_dataframe, 'Unique_key')
    p_dataframe['Substation'] = number2string_deleteDecimal(p_dataframe, 'Substation')
    p_dataframe['Bay_number'] = number2string_deleteDecimal(p_dataframe, 'Bay_number')
    # Kolonların csv'de sıralanması
    return p_dataframe[[
        'ASSETID',
        'Unique_key',
        'Status_Indicator',
        'Phase_Designation',
        'B3_Text',
        'Substation',
        'TeAR',
        'NormalState',
        'Type',
        'Switch_to_ground',
        'Bay_number',
        'Nominal_Voltage',
        'AmpSC_',
        'GIS_ID',
        'Point_X',
        'Point_Y',
        'Xwgs84',
        'Ywgs84'
    ]]

import random

# CSV dosyasndan DXF olusturma fonksiyonu
def export_DXF(csv_path):
    dxf_folder = "..\\Outputs\\DXF"
    # dxf ve geodatabase klasorunu sil
    # dxf dosyasinin silinmesinin sebebi dxf tekrar olustururken var olan dxf dosyasina eklenmesi ve dosya boyutunu sisirmesidir.
    dxf_path = "..\\Outputs\\DXF\\switch.DXF"
    gdb_path = "output_data.gdb"
    point_feature_class_dir = "\\switch"
    point_feature_class = "switch"
    # GDB klasörü oluştur
    if not os.path.exists(dxf_folder):
        os.makedirs(dxf_folder)
    dosya_sil(dxf_path)
    # feature class'in koordinat sistemi degeri
    spatial_ref = arcpy.Describe(get_input() + '\ELE_DD_SEPARATOR_POINT').spatialReference
    # DXF dosya donusumu: donusumu tamamlanan csv dosyasindan X ve Y koordinatlarinin DXF'ye donusumu
    # Geodatabase'de transformer feature class olusturulmasi
    featureclass_path = 'output_data.gdb\\switch'
    # current directory
    directory_gdb = os.getcwd()
    # gdb klasoru varsa sil
    # Donusum islemi
    print("Koordinat sistemi: ", spatial_ref.Name)
    if arcpy.Exists(gdb_path):
        arcpy.Delete_management(gdb_path)
        print ("gdb path has been deleted.")
    # csv'den dxf'ye donusum islemi
    # 1- ilk olarak csv'den X ve Y koordinatlari cekilip output.gdb'de point feature class olusturulur
    # gdb olustur
    arcpy.CreateFileGDB_management(directory_gdb, gdb_path)
    if arcpy.Exists(featureclass_path):
        arcpy.Delete_management(featureclass_path)
    arcpy.MakeXYEventLayer_management(csv_path, 'Point_X', 'Point_Y', featureclass_path, spatial_ref, '#')
    arcpy.FeatureClassToFeatureClass_conversion(featureclass_path, gdb_path, point_feature_class)
    print ("CSV tablosundan Feature Class olusturma islemi tamamlandi.")
    # 2- olusturulan feature class DXF'ye donusturulur.
    arcpy.ExportCAD_conversion(gdb_path + point_feature_class_dir, 'DXF_R2013', dxf_path,
                               'Ignore_Filenames_in_Tables', 'Overwrite_Existing_Files', '#')
    # işlem sonucu oluşan xml'nin silinmesi
    dosya_sil(dxf_path + ".xml")
    print "dxf dosya donusumu tamamlandi."