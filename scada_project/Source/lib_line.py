# -*- coding: utf8 -*-
# Author : Edip Ahmet TASKIN, Universal Yazilim A.Ş.
from common import *
import pandas as pd

# Veritabanindan veri cekme ve ilk dataframeyi olusturma
def dataframe_from_database():
    sql = (""" 
    SELECT
    cable.OBJECTID,
    cable.SHAPE.STEndPoint().STX AS Xend,
    cable.SHAPE.STEndPoint().STY AS Yend,
    cable.SHAPE.STStartPoint().STX AS Xstart,
    cable.SHAPE.STStartPoint().STY AS Ystart,
    
     cable.ASSETID,
     cable.CLASS,
     cable.LENGTH,
     cable.TYP,
     cable.Z_Hat_Adi,
     cable.CROSS_SECTION,
     cable.OPERATING_VOLTAGE,
     cable.OPERATIONALCENTER_REF,
    bnd.Z_TEAR_ID as TeAR
    FROM ELE_CABLE_evw cable
    left join BNDRY_OPERATIONAL_CENTER bnd on cable.OPERATIONALCENTER_REF = bnd.CENTER_CODE
    """)
    # Okunan degerleri dataframe olarak aktar:
    p_dataframe = pd.read_sql_query(sql, sqlalchemy_connection())
    return p_dataframe

def rename_dataframe(p_dataframe):
    return p_dataframe.rename(columns={
        'OPERATING_VOLTAGE': 'NumVolt',
        'CLASS': 'Type',
        'TYP': 'Conductor_Material',
        'LENGTH': 'Length',
        'CROSS_SECTION': 'Cond_size',
        'Z_Hat_Adi': 'B3_Text'
    }, inplace=True)

# Değeri CCODE olan ve LOOKUP tablosuna bağlı olan kolonların değerlerinin atanması
def gis_to_scada(p_dataframe):
    gis_description_to_scada(p_dataframe, "ELE_CABLE_evw", 'CLASS', 'Insulation_type')
    gis_description_to_scada(p_dataframe, "ELE_CABLE_evw", 'TYP', 'Conductor_Material')
    gis_description_to_scada(p_dataframe, "ELE_CABLE_evw", 'CLASS', 'Type')
    gis_description_to_scada(p_dataframe, "ELE_CABLE_evw", 'CROSS_SECTION', 'Cond_size')
    gis_description_to_scada(p_dataframe, "ELE_CABLE_evw", 'OPERATING_VOLTAGE', 'NumVolt')

def GIS_ID(p_dataframe):
    p_dataframe['ASSETID'] = desimal_sil(p_dataframe,'ASSETID')
    return "LINE00000000" + p_dataframe['ASSETID'].astype(str)

def sabit_degerler(p_dataframe):
    p_dataframe['Phase_Designation'] = '7'
    p_dataframe['Status_indicator'] = "3"
    p_dataframe['Insulation_type'] = p_dataframe['Type']
    p_dataframe['Cond_config'] = ""
    p_dataframe["Length"] = p_dataframe["Length"].astype(str)
    p_dataframe["Length"] = yerdegistir(p_dataframe, "Length", "0.0", "")


def multiple(x):
    return x*1000

def final_dataframe(p_dataframe):
    p_dataframe.replace(to_replace=[r"\\t|\\n|\\r", "\t|\n|\r"], value=["", ""], regex=True, inplace=True)
    p_dataframe.loc[p_dataframe['Length'].str.contains('nan'), 'Length'] = ''
    p_dataframe['NumVolt'] = number2string_deleteDecimal(p_dataframe, 'NumVolt')
    p_dataframe = p_dataframe.fillna('')
    return p_dataframe[[
        'ASSETID',
        'Status_indicator',
        'Phase_Designation',
        'Insulation_type',
        'Conductor_Material',
        'B3_Text',
        'Length',
        'Type',
        'NumVolt',
        'Cond_config',
        'Cond_size',
        'GIS_ID',
        'TeAR',
        'Xstart',
        'Ystart',
        'Xend',
        'Yend',
        'Xwgs84_start',
        'Ywgs84_start',
        'Xwgs84_end',
        'Ywgs84_end'
    ]]