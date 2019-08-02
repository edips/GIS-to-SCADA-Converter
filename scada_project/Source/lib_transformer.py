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

# # Yenilikler
# Voltage_Low kolonu canlı veritabanında tamamlanmıştır.
# Tear kolonunun kontrol edilmesi gerekir. Selim Bey bu konuda lokasyon bazlı Tear verileceğini söyledi.
# Substation kolonu verilecek karar doğrultusunda düzeltilecektir.
# _____________________________________________________________________________
# Veritabanindan veri cekme ve ilk dataframeyi olusturma
def dataframe_from_database():
    sql = """\
    select 
    ASSETID, 
    SHAPE.STX as Point_X, 
    SHAPE.STY as Point_Y, 
    CITY_CODE,STATION_REF,
    STATUS,
    SUSCEPTANCE,
    IMPEDANCE_ZERO_REACTANCE,
    IMPEDANCE_ZERO_RESISTANCE,
    OPERATIONALCENTER_REF,
    PHASE_COUNT,
    CONNECTION_GROUP,
    PRIMARY_VOLTAGE,
    SECONDARY_VOLTAGE,
    SECONDARY_MAXLEVEL_VOLTAGE,
    NUMBER_OF_TAPS_SECONDARY,
    SECONDARY_MINLEVEL_VOLTAGE,
    NUMBER_OF_TAPS_PRIMARY,
    OWNER,
    CURRENT_TAP_SECONDARY,
    CURRENT_TAP_PRIMARY,
    POWER,
    NAME_NUMBER,
    PRIMARY_MAXLEVEL_VOLTAGE,
    EARTHING_PRIMER_RESISTANCE,
    EARTHING_PRIMER_REACTANCE,
    EARTHING_SECONDER_RESISTANCE,
    EARTHING_SECONDER_REACTANCE,
    IMPEDANCE_POSITIVE_RESISTANCE,
    IMPEDANCE_POSITIVE_REACTANCE,
    PRIMARY_MINLEVEL_VOLTAGE,
    CONDUCTANCE
    from ELE_DD_TRANSFORMER_evw
    """
    # Okunan degerleri dataframe olarak aktar:
    p_dataframe = pd.read_sql_query(sql, sqlalchemy_connection())
    return p_dataframe

# dataframe kolonlarini yeniden adlandir
def rename_dataframe(p_dataframe):
    # GIS'te bulunan alanlarin isimlerinin CSV'deki alan isimlerine uyarlanmasi
    p_dataframe.rename(columns={
        'EARTHING_PRIMER_RESISTANCE': 'Resist_HN_G',
        'EARTHING_PRIMER_REACTANCE': 'React_HN_G',
        'EARTHING_SECONDER_REACTANCE': 'React_LN_G',
        'IMPEDANCE_POSITIVE_RESISTANCE': 'Resist_POS',
        'IMPEDANCE_POSITIVE_REACTANCE': 'React_POS',
        'POWER': 'Rated_Power',
        'NAME_NUMBER': 'B3_Text',
        'STATION_REF': 'Substation',
        'IMPEDANCE_ZERO_REACTANCE': 'React_Zero',
        'IMPEDANCE_ZERO_RESISTANCE': 'Resist_Zero',
        'CONNECTION_GROUP': 'Winding_Connection_Type',
        'PRIMARY_VOLTAGE': 'Voltage_High',
        'SECONDARY_VOLTAGE': 'Voltage_Low',
        'CURRENT_TAP_SECONDARY': 'Normal_Tap_Pos_Low',
        'CURRENT_TAP_PRIMARY': 'Normal_Tap_Pos_High',
        'EARTHING_SECONDER_RESISTANCE': 'Resist_LN_G'
    }, inplace=True)
    return p_dataframe

# Facility_ID kolonu istasyonun icinde kumulatif olarak toplama yontemine gore siralanir
def facility_id_query(p_dataframe):
    p_dataframe['Facility_ID'] = p_dataframe.groupby('Substation').cumcount() + 1
    p_dataframe.loc[p_dataframe['Substation'].isnull(), 'Facility_ID'] = p_dataframe['Substation']
    return p_dataframe["Facility_ID"]

# Tear icin veritabanina veri atandiginda degerler kolona gelecektir.
# NOT: TeAR kolonu ELE_DD_TRANSFORMER'de açıldığında aşağıdaki fonksiyon iptal edilir.
# Yukarıdaki SQL sorgusuna TeAR kolonu açılarak dataframe'ye dahil edilir.
def tear_kolonu(p_dataframe):
    sql = """ select trafo.ASSETID, bnd.Z_TEAR_ID as TeAR from ELE_DD_TRANSFORMER_evw trafo
        left join BNDRY_OPERATIONAL_CENTER bnd on trafo.OPERATIONALCENTER_REF = bnd.CENTER_CODE """
    # Okunan degerleri dataframe olarak aktar:
    df_tear = pd.read_sql_query(sql, sqlalchemy_connection())
    # df_tear ile p_dataframe arasinda ASSETID kullanilarak left_join islemi yapilmasi
    p_dataframe = pd.merge(p_dataframe, df_tear, how='left', left_on=['ASSETID'], right_on=['ASSETID'])
    p_dataframe["TeAR"] = p_dataframe["TeAR"].fillna('')
    p_dataframe["TeAR"] = p_dataframe["TeAR"].astype(str)
    return p_dataframe["TeAR"]

# gis_description_to_scada fonksiyonunun daha az kodla çalışması için
# gis_description_to_scada2 fonksiyonu liste olarak oluşturulmuştur.
def gis_description_to_scada2(p_dataframe):
    gis_scada_layer_list = [('PRIMARY_VOLTAGE', 'Voltage_High'),
                            ('SECONDARY_VOLTAGE', 'Voltage_Low'),
                            ('CONNECTION_GROUP', 'Winding_Connection_Type'),
                            ('CURRENT_TAP_PRIMARY', 'Normal_Tap_Pos_High'),
                            ('IMPEDANCE_POSITIVE_REACTANCE', 'React_POS'),
                            ('EARTHING_SECONDER_REACTANCE', 'React_LN_G'),
                            ('IMPEDANCE_POSITIVE_RESISTANCE', 'Resist_POS'),
                            ('IMPEDANCE_ZERO_RESISTANCE', 'Resist_Zero'),
                            ('CONDUCTANCE', 'CONDUCTANCE'),
                            ('SUSCEPTANCE', 'SUSCEPTANCE'),
                            ("NUMBER_OF_TAPS_SECONDARY","NUMBER_OF_TAPS_SECONDARY"),
                            ('CURRENT_TAP_SECONDARY', 'Normal_Tap_Pos_Low'),
                            ("SECONDARY_MINLEVEL_VOLTAGE","SECONDARY_MINLEVEL_VOLTAGE"),
                            ('EARTHING_PRIMER_RESISTANCE', 'Resist_HN_G'),
                            ('EARTHING_PRIMER_REACTANCE', 'React_HN_G'),
                            ("SECONDARY_MAXLEVEL_VOLTAGE","SECONDARY_MAXLEVEL_VOLTAGE"),
                            ('PRIMARY_MAXLEVEL_VOLTAGE', 'PRIMARY_MAXLEVEL_VOLTAGE'),
                            ('EARTHING_SECONDER_RESISTANCE', 'Resist_LN_G'),
                            ('PRIMARY_MINLEVEL_VOLTAGE', 'PRIMARY_MINLEVEL_VOLTAGE'),
                            ('NUMBER_OF_TAPS_PRIMARY', 'NUMBER_OF_TAPS_PRIMARY'),
                            ('POWER', 'Rated_Power')
                            ]
    for i in gis_scada_layer_list:
        # common.py'deki gis_description_to_scada fonksiyonu, listeye göre döngüde çalışır.
        gis_description_to_scada(p_dataframe,"ELE_DD_TRANSFORMER_evw", i[0], i[1])
    return p_dataframe
# EMPEDANS KOLONLARININ AKTARIMI
# 1- OEDAŞ tarafından verilen Empedans excel tablosu ayırıcısı virgül olacak şekilde csv'ye dönüştürülür.
# CSV'ye dönüştürülmesinin sebebi EXCEL'den doğrudan dataframe'ye aktardığında unicode ile ilgili sorunların çıkmasıdır.
def empedance_dataframe_from_csv(dataframe,csv_path):
    # empedans tablosunu dataframeye aktar
    df_empedans = read_csv(csv_path)
    # Tiplerin ayni olmasi icin stringe donusturulur
    df_empedans["Primary_Voltage"] = df_empedans["Primary_Voltage"].astype(str)
    df_empedans["Power"] = df_empedans["Power"].astype(str)
    # kolondaki bosluklari sil
    df_empedans["Power"] = kolondaki_bosluklari_sil(df_empedans, "Power")
    # numerik degerlerde bosluklarin silinmesi gerekir
    dataframe["Rated_Power"] = kolondaki_bosluklari_sil(dataframe, "Rated_Power")
    # unicode atamasinda sorun oldugu icin Rated_Power2 adinda yeni bir kopyası olusturuldu,
    # bu kolon geçici olarak empedans degerlerinin ayarlanmasinda kullanılır
    dataframe['Rated_Power2'] = kolondaki_unicodu_sil(dataframe, "Rated_Power")
    # Rated_Power2 kolonunda kVA içeren kayıtların nümerik değerini çıkar
    dataframe.loc[dataframe['Rated_Power2'].str.contains('kVA'), 'Rated_Power2'] =\
        dataframe['Rated_Power2'].astype(str).str.extract('(\d+(?:\.\d+)?)', expand=False)
    # Rated_Power2 kolonunda mVA içeren kayıtların nümerik değerini çıkar
    dataframe.loc[dataframe['Rated_Power2'].str.contains('MVA', na=False), 'Rated_Power2'] =\
        dataframe['Rated_Power2'].astype(str).str.extract('(\d+(?:\.\d+)?)', expand=False) + "0000"

    dataframe['Rated_Power2'] = dataframe['Rated_Power2'].astype(str)
    # Power kolonunda kVA içeren kayıtların nümerik değerini çıkar
    df_empedans.loc[df_empedans['Power'].str.contains('kVA', case=False), 'Power'] =\
        df_empedans['Power'].astype(str).str.extract('(\d+(?:\.\d+)?)', expand=False)
    # Power kolonunda mVA içeren kayıtların nümerik değerini çıkar
    df_empedans.loc[df_empedans['Power'].str.contains('MVA', na=False, case=False), 'Power'] =\
        df_empedans['Power'].astype(str).str.extract('(\d+(?:\.\d+)?)', expand=False) + "0000"
    # power_voltage kolonu Primary_Voltage ile Power kolonlarinin birlesimiyle tuple sekline donusturulur.
    df_empedans["power_voltage"] = iki_kolonu_tuple_olarak_birlestir(df_empedans, "power_voltage", 'Primary_Voltage',"Power")
    # Empedans tablosundan cekilen dataframe'nin son hali
    return df_empedans[["power_voltage", "Result", "Type", "Sequence"]]

def empedans_kolonlarini_olustur(p_dataframe, df_empedans, empedance_type, empedance_sequence, empedance_column ):
    # Empedans kolonlarının dönüşümü için referans alınan iki kolon listeye çevrilir.
    resist_positive_df = df_empedans[(df_empedans.Type == empedance_type) & (df_empedans.Sequence == empedance_sequence)]
    resist_positive_df = resist_positive_df[["power_voltage", 'Result']]
    p_dataframe[empedance_column] = iki_kolonu_list_olarak_birlestir(p_dataframe, empedance_column, "Voltage_High2","Rated_Power2")
    p_dataframe[empedance_column] =p_dataframe[empedance_column].map(resist_positive_df.set_index("power_voltage")['Result']).fillna('')
# # Empedans kolonlarinin olusturulmasi
def empedans_read_from_csv(p_dataframe):
    # React_POS icin once Voltage_High ve Rated_Power kolonlari birlestirilir.
    # Extract numeric values from string:
    p_dataframe["Voltage_Low"] = extract_numeric_value(p_dataframe, "Voltage_Low")
    p_dataframe["Voltage_High"] = extract_numeric_value(p_dataframe, "Voltage_High")
    # unicode atamasinda sorununu giderme islemi
    p_dataframe['Rated_Power2'] = kolondaki_unicodu_sil(p_dataframe, "Rated_Power")
    p_dataframe.loc[p_dataframe['Rated_Power2'].str.contains('kVA'), 'Rated_Power2'] =\
        p_dataframe['Rated_Power2'].astype(str).str.extract('(\d+(?:\.\d+)?)', expand=False)

    p_dataframe.loc[p_dataframe['Rated_Power2'].str.contains('MVA', na=False), 'Rated_Power2'] = \
        p_dataframe['Rated_Power2'].astype(str).str.extract('(\d+(?:\.\d+)?)', expand=False) + "0000"

    p_dataframe['Rated_Power2'] = p_dataframe['Rated_Power2'].astype(str)
    p_dataframe['Voltage_Low2'] = kolondaki_unicodu_sil(p_dataframe, "Voltage_Low")
    p_dataframe['Voltage_High2'] = kolondaki_unicodu_sil(p_dataframe, "Voltage_High")
    p_dataframe['React_POS'] = iki_kolonu_list_olarak_birlestir(p_dataframe, "React_POS", "Voltage_High2","Rated_Power2")
    # Empendans csv tablosunu df_empedans datarfameye aktar
    df_empedans = empedance_dataframe_from_csv(p_dataframe, 'Transformer_Impedance_Referance.csv')
    # Empedans kolonlarini olustur
    empedans_kolonlarini_olustur(p_dataframe, df_empedans, 'Reactance', 'Zero', 'React_Zero')
    empedans_kolonlarini_olustur(p_dataframe, df_empedans, 'Reactance', 'Positive', 'React_POS')
    empedans_kolonlarini_olustur(p_dataframe, df_empedans, 'Resistance', 'Zero', 'Resist_Zero')
    empedans_kolonlarini_olustur(p_dataframe, df_empedans, 'Resistance', 'Positive', "Resist_POS")
    # Rated_Power -------------------------------------------------------------------------
    p_dataframe['Rated_Power'] = p_dataframe['Rated_Power'].str.replace(' ', '')
    p_dataframe = p_dataframe.replace(np.nan, '')
    # Rated_Power-------------------------------------------------------------------------
    # kVA içeren kayıtların nümerik değerlerinin çıkartılması
    p_dataframe.loc[p_dataframe['Rated_Power'].str.contains('kVA'), 'Rated_Power'] =\
        p_dataframe['Rated_Power'].astype(str).str.extract('(\d+(?:\.\d+)?)', expand=False)
    # MVA içeren kayıtların nümerik değerlerinin çıkartılması
    p_dataframe.loc[p_dataframe['Rated_Power'].str.contains('MVA', na=False), 'Rated_Power'] =\
        p_dataframe['Rated_Power'].astype(str).str.extract('(\d+(?:\.\d+)?)', expand=False) + "0000"

    p_dataframe['Rated_Power'] = p_dataframe['Rated_Power'].astype(str)
    return p_dataframe

# sabit degerlerin atanmasi:
def sabit_degerler(p_dataframe):
    p_dataframe['Neutral_Volt_Low'] = "0"
    p_dataframe['Neutral_Volt_High'] = ""
    p_dataframe['Neutral_Tap_Pos_High'] = ""
    p_dataframe['Neutral_Tap_Pos_Low'] = ""
    # bilgi: dataframe AND operatoru kullanimi: (df['col_name'].str.contains('apple')) & (df['col_name'].str.contains('banana'))
    p_dataframe['Status_Indicator'] = 3
    p_dataframe['Phase_Designation'] = 7
    p_dataframe['Transformer_Bank'] = "Y"
    # TeAR kolonu veritabanında ELE_DD_TRANSFORMER'e eklendiğinde aşağıdaki kod silinecek ve
    # TeAR kolonu doğrudan sorgudan veri çekecek.
    p_dataframe['TeAR'] = tear_kolonu(p_dataframe)
    p_dataframe['GIS_ID'] = "TRF000000000" + p_dataframe['ASSETID'].astype(str)
    p_dataframe['Lim_Step_Up_Low'] = ""
    p_dataframe['Lim_Step_Down_Low'] = ""
    # İstasyonda trafo sayısını belirten Facility_ID kolonunun değerleri
    p_dataframe['Facility_ID'] = facility_id_query(p_dataframe)
    p_dataframe["Lim_Step_Down_High"] = ""
    # Analiz dökümanına göre belirlenen matematiksel işlemlerin yapılması amacıyla ilgili kolonlar nümerike çevrilir.
    p_dataframe["Lim_Step_Down_High"] = p_dataframe["Lim_Step_Down_High"].convert_objects(convert_numeric=True)
    p_dataframe["Normal_Tap_Pos_High"] = p_dataframe["Normal_Tap_Pos_High"].convert_objects(convert_numeric=True)
    p_dataframe["NUMBER_OF_TAPS_PRIMARY"] = p_dataframe["NUMBER_OF_TAPS_PRIMARY"].convert_objects(convert_numeric=True)
    p_dataframe['Lim_Step_Down_High'] = p_dataframe["Normal_Tap_Pos_High"] - 1
    p_dataframe['Lim_Step_Up_High'] = ""
    p_dataframe['Lim_Step_Up_High'] = p_dataframe["NUMBER_OF_TAPS_PRIMARY"] - p_dataframe["Normal_Tap_Pos_High"]
    p_dataframe["PRIMARY_MAXLEVEL_VOLTAGE"] = p_dataframe["PRIMARY_MAXLEVEL_VOLTAGE"].convert_objects(convert_numeric=True)
    p_dataframe["PRIMARY_MINLEVEL_VOLTAGE"] = p_dataframe["PRIMARY_MINLEVEL_VOLTAGE"].convert_objects(convert_numeric=True)
    p_dataframe["SECONDARY_MAXLEVEL_VOLTAGE"] = p_dataframe["SECONDARY_MAXLEVEL_VOLTAGE"].convert_objects(convert_numeric=True)
    p_dataframe["SECONDARY_MINLEVEL_VOLTAGE"] = p_dataframe["SECONDARY_MINLEVEL_VOLTAGE"].convert_objects(convert_numeric=True)
    p_dataframe["NUMBER_OF_TAPS_SECONDARY"] = p_dataframe["NUMBER_OF_TAPS_SECONDARY"].convert_objects(convert_numeric=True)
    p_dataframe["NUMBER_OF_TAPS_PRIMARY"] = p_dataframe["NUMBER_OF_TAPS_PRIMARY"].convert_objects(convert_numeric=True)
    p_dataframe['Step_Size_High'] = ((p_dataframe["PRIMARY_MAXLEVEL_VOLTAGE"] - p_dataframe["PRIMARY_MINLEVEL_VOLTAGE"])
                                    / p_dataframe["NUMBER_OF_TAPS_PRIMARY"]) / p_dataframe["PRIMARY_MAXLEVEL_VOLTAGE"]
    p_dataframe['SECONDARY_MAXLEVEL_VOLTAGE'] = p_dataframe['SECONDARY_MAXLEVEL_VOLTAGE'].replace('', np.nan)
    p_dataframe['SECONDARY_MINLEVEL_VOLTAGE'] = p_dataframe['SECONDARY_MINLEVEL_VOLTAGE'].replace('', np.nan)
    p_dataframe.SECONDARY_MAXLEVEL_VOLTAGE = p_dataframe.SECONDARY_MAXLEVEL_VOLTAGE.apply(float)
    p_dataframe.SECONDARY_MINLEVEL_VOLTAGE = p_dataframe.SECONDARY_MINLEVEL_VOLTAGE.apply(float)
    p_dataframe['Step_Size_Low'] = p_dataframe["SECONDARY_MAXLEVEL_VOLTAGE"] - p_dataframe["SECONDARY_MINLEVEL_VOLTAGE"]

# Kolonlarin siralanmasi ve dataframenin son halinin csv'ye export edilmesi
def final_dataframe(p_dataframe):
    # B3_Text'te bazı veriler nedense enter'e basılmış gönürüyor, bude csv'ye aktarırken veri kaymasına sebep oluyor.
    # Bu hatanın giderilmesi için aşağıdaki kod uygulanmıştır.
    p_dataframe.replace(to_replace=[r"\\t|\\n|\\r", "\t|\n|\r"], value=["", ""], regex=True, inplace= True)
    # Dataframe'de nümerik değerler csv'ye aktarılırken ondalık kısım dahil olarak aktarılmakta. Bu durum
    # integer değerler için sorun oluşturuyor. Ondalik kisimi olmamasi gereken serilerin ondalik degerlerinin silinmesi:
    p_dataframe["Facility_ID"] = p_dataframe['Facility_ID'].astype(str).replace('\.0', '', regex=True)
    p_dataframe["Substation"] = p_dataframe['Substation'].astype(str).replace('\.0', '', regex=True)
    # Stringe dönüştürülen değerlerin NULL değerleri "nan" olarak dönüştürülüyor. "nan"'ı boşluğa çevirme kodu:
    p_dataframe.loc[p_dataframe['Substation'].str.contains('nan'), 'Facility_ID'] = ''
    p_dataframe.loc[p_dataframe['Substation'].str.contains('nan'), 'Substation'] = ''
    # NULL değerlerin tamamının boşluğa dönüşümü
    p_dataframe = p_dataframe.fillna('')
    # Kolonların csv'de sıralanması
    return p_dataframe[[
        'ASSETID',
        'Status_Indicator',
        'Phase_Designation',
        'Facility_ID',
        'Substation',
        'TeAR',
        'Transformer_Bank',
        'Winding_Connection_Type',
        'Voltage_High',
        'Voltage_Low',
        'Resist_HN_G',
        'React_HN_G',
        'Resist_LN_G',
        'React_LN_G',
        'Resist_POS',
        'React_POS',
        'CONDUCTANCE',
        'SUSCEPTANCE',
        'Resist_Zero',
        'React_Zero',
        'Normal_Tap_Pos_Low',
        'Lim_Step_Up_Low',
        'Lim_Step_Down_Low',
        'Step_Size_Low',
        'Neutral_Tap_Pos_Low',
        'Neutral_Volt_Low',
        'Normal_Tap_Pos_High',
        'Lim_Step_Up_High',
        'Lim_Step_Down_High',
        'Step_Size_High',
        'Neutral_Tap_Pos_High',
        'Neutral_Volt_High',
        'GIS_ID',
        'B3_Text',
        'Point_X',
        'Point_Y',
        "Xwgs84",
        "Ywgs84",
        'Rated_Power',
    ]]

# CSV dosyasndan DXF olusturma fonksiyonu
def export_DXF():
    # DXF klasörünün oluşturulması
    if not os.path.exists("..\\Outputs\\DXF"):
        os.makedirs("..\\Outputs\\DXF")
    # GDB mevcutsa sil
    gdb_folder = 'output_data.gdb'
    if os.path.exists(gdb_folder):
        shutil.rmtree(gdb_folder)
    # path parametreleri
    dxf_path = "..\\Outputs\\DXF\\trafo.DXF"
    csv_path = "..\\Outputs\\transformar.csv"
    point_feature_class_dir = "\\trafo"
    point_feature_class = "trafo"
    # DXF dosyası mevcutsa sil
    dosya_sil(dxf_path)
    # feature class'in koordinat sistemi degeri
    spatial_ref = arcpy.Describe(get_input() + '\ELE_DD_TRANSFORMER').spatialReference
    # DXF dosya donusumu: donusumu tamamlanan csv dosyasindan X ve Y koordinatlarinin DXF'ye donusumu
    # Geodatabase'de transformer feature class olusturulmasi
    featureclass_path = 'output_data.gdb\\transformer'
    # current directory
    directory_gdb = os.getcwd()
    print("Koordinat sistemi: ", spatial_ref.Name)
    # gdb olustur
    arcpy.CreateFileGDB_management(directory_gdb, gdb_folder)
    # csv'den dxf'ye donusum islemi
    # 1- ilk olarak csv'den X ve Y koordinatlari cekilip output.gdb'de point feature class olusturulur.
    if arcpy.Exists(gdb_folder + point_feature_class_dir):
        pass
    else:
        arcpy.MakeXYEventLayer_management(csv_path, 'Point_X', 'Point_Y', featureclass_path, spatial_ref,'#')
        arcpy.FeatureClassToFeatureClass_conversion(featureclass_path, gdb_folder, point_feature_class)
        print ("CSV tablosundan Feature Class olusturma islemi tamamlandi.")
    # 2- olusturulan feature class DXF'ye donusturulur.
    arcpy.ExportCAD_conversion(gdb_folder + point_feature_class_dir, 'DXF_R2013', dxf_path,
                               'Ignore_Filenames_in_Tables', 'Overwrite_Existing_Files', '#')
    # işlem sonucu oluşan xml'nin silinmesi
    dosya_sil(dxf_path + ".xml")
    print("DXF donusumu tamamlandi.")