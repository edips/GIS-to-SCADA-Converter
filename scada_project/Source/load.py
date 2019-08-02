# -*- coding: utf8 -*-
# Author : Edip Ahmet TASKIN, Universal Yazilim A.Ş.
from lib_load import *
import csv

def load():
    # veritabanindan dataframe olustur.
    p_dataframe = dataframe_from_database()
    # Nominal_Voltage kolonundan desimali sil
    desimal_sil(p_dataframe, "Nominal_Voltage")
    # sabit degerleri ata
    sabit_degerler(p_dataframe)
    # NULL degerleri bosluk yap
    p_dataframe = p_dataframe.fillna('')
    # stringe cevrilmis olan null'lar yerine nan gelir, nanlari bosluk yap
    p_dataframe = p_dataframe.replace('nan', '', regex=True)
    # Substation bos oldugunda  bu kolonlarinda bos gelmesi islemi
    # GIS_ID
    p_dataframe['GIS_ID'] = "LOAD00000000" + p_dataframe['ASSETID'].astype(str)
    # Substation'a bagli olan BARA_ASSETID kolonu icin Substation bos oldugunda  bu kolon bos gelmelidir
    # Normal_Voltage'nin CCODE yerine OPERATIONAL_VOLTAGE'nin descriptionunu cekmesi
    gis_description_to_scada(p_dataframe, "ELE_HOUSE_CONNECTION_evw", "OPERATING_VOLTAGE", 'Nominal_Voltage')
    # Dataframe'nin son hali, kolonlari yerlestirilmesi
    p_dataframe = final_dataframe(p_dataframe)
    # csv ve excel'e export islemi
    dataframe_export(p_dataframe)
    # islem bittiginde output klasoru acilir
    open_output_folder()