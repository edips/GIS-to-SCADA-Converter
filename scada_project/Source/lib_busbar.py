# -*- coding: utf8 -*-
# Author : Edip Ahmet TASKIN, Universal Yazilim A.Ş.
from common import *
import pandas as pd

# Veritabanindan veri cekme ve ilk dataframeyi olusturma
def dataframe_from_database():
    sql = (""" 
    SELECT 
    busbar.SHAPE.STEndPoint().STX AS Xend,
    busbar.SHAPE.STEndPoint().STY AS Yend,
    busbar.SHAPE.STStartPoint().STX AS Xstart,
    busbar.SHAPE.STStartPoint().STY AS Ystart,
    busbar.ASSET_NUMBER AS B3_Text,
    busbar.ASSETID as ASSETID,
    busbar.ENABLED as ENABLED,
    busbar.STATION_REF as STATION_REF,
    busbar.OPERATING_VOLTAGE as OPERATING_VOLTAGE,
    bnd.Z_TEAR_ID as TeAR
    FROM ELE_DD_CONNECTOR_evw busbar
    left join BNDRY_OPERATIONAL_CENTER bnd on busbar.OPERATIONALCENTER_REF = bnd.CENTER_CODE 
    WHERE SUBTYP='2'
    """)
    # Okunan degerleri dataframe olarak aktar:
    p_dataframe = pd.read_sql_query(sql, sqlalchemy_connection())
    return p_dataframe

def rename_dataframe(p_dataframe):
    return p_dataframe.rename(columns={
        'STATION_REF': 'Substation',
        'ENABLED': 'Enabled',
        'OPERATING_VOLTAGE': 'Nominal_Voltage',
    }, inplace=True)

# Facility_ID kolonuna kümilatif olarak değer atanması
# TODO; Facility_ID kolonuna OEDAŞ tarafından değer verilecek ve
# TODO; Facility_ID sorguya dahil edilerek aşağıdaki fonksiyon silinecek
def facility_id(p_dataframe):
    p_dataframe["Facility_ID"] = p_dataframe.groupby('Substation').cumcount() + 1

# B3_Text kolonu analiz dökümanına göre Facility_ID değerine göre BB1a, BB2A... BB8A gibi değerler alabilir.
def B3_Text(p_dataframe):
    p_dataframe['B3_Text'] = p_dataframe["Facility_ID"]
    p_dataframe.loc[p_dataframe.Facility_ID == 1, 'B3_Text'] = "BB1A"
    p_dataframe.loc[p_dataframe.Facility_ID == 2, 'B3_Text'] = "BB2A"
    p_dataframe.loc[p_dataframe.Facility_ID == 3, 'B3_Text'] = "BB3A"
    p_dataframe.loc[p_dataframe.Facility_ID == 4, 'B3_Text'] = "BB4A"
    p_dataframe.loc[p_dataframe.Facility_ID == 5, 'B3_Text'] = "BB5A"
    p_dataframe.loc[p_dataframe.Facility_ID == 6, 'B3_Text'] = "BB6A"
    p_dataframe.loc[p_dataframe.Facility_ID == 7, 'B3_Text'] = "BB7A"
    p_dataframe.loc[p_dataframe.Facility_ID == 8, 'B3_Text'] = "BB8A"
    return p_dataframe['B3_Text']

def ondaliklari_int_yap(p_dataframe):
    # Ondalik kisimi olmamasi gereken serilerin ondalik degerlerinin silinmesi:
    desimal_sil(p_dataframe, "Facility_ID")

def sabit_degerler(p_dataframe):
    p_dataframe['Phase_Designation'] = 7
    p_dataframe['Busbar_Type'] = 'M'
    p_dataframe['Status_Indicator'] = 3

def final_dataframe(p_dataframe):
    p_dataframe.replace(to_replace=[r"\\t|\\n|\\r", "\t|\n|\r"], value=["", ""], regex=True, inplace=True)
    return p_dataframe[[
        'ASSETID',
        'Xstart',
        'Ystart',
        'Xend',
        'Yend',
        'Xwgs84_start',
        'Ywgs84_start',
        'Xwgs84_end',
        'Ywgs84_end',
        'Enabled',
        'Status_Indicator',
        'Phase_Designation',
        'Facility_ID',
        'B3_Text',
        'Substation',
        'Busbar_Type',
        'Nominal_Voltage',
        'GIS_ID',
        'BARA_ASSETID',
        'TeAR'
    ]]

# dataframe'yi csv ve excel'e aktar
def dataframe_export(p_dataframe):
    # p_dataframe.to_csv(r".\Outputs\busbar.csv", sep=';', encoding='utf-8', index=False)
    p_dataframe.to_csv(r"..\Outputs\busbar.csv", sep=';', encoding='utf-8', index=False)
    print ("CSV donusumu tamamlandi-----------busbar.csv")
    # p_dataframe.to_excel(r".\Outputs\busbar.xlsx", 'Sheet1', p_dataframe.columns, index=False)
    p_dataframe.to_excel(r"..\Outputs\busbar.xlsx", 'Sheet1', p_dataframe.columns, index=False)
    print ("Excel donusumu tamamlandi-----------busbar.xlsx")