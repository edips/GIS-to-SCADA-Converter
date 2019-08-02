# -*- coding: utf8 -*-
# Author : Edip Ahmet TASKIN, Universal Yazilim A.Ş.
from lib_station import *
def station():
    # Dataframe'in olusturulmasi ve databaseden gerekli veri almasi
    p_dataframe = dataframe_from_database()
    wgs84_kolonlari(p_dataframe)
    # Alan degeri description olan alanlarin fonksiyon olarak calistirilmasi:
    gis_description_to_scada(p_dataframe,"ELE_STATION_evw","RTU_type","Telemetered")
    gis_description_to_scada(p_dataframe, "ELE_STATION_evw", "CLASS", "Type")
    # Sabit degerlerli kolonlarin olusturulmasi
    sabit_degerler(p_dataframe)
    # Alanlarin siralanmasi
    p_dataframe = final_dataframe(p_dataframe)
    # Dataframe'nin csv'ye donusumu
    p_dataframe.to_csv(r"..\Outputs\station.csv", sep=';', encoding='utf-8', na_rep="", index=False)
    print ("CSV donusumu tamamlandi-----------station.csv")
    # Dataframe'nin excele donusumu
    p_dataframe.to_excel(r"..\Outputs\station.xlsx", 'Sheet1', p_dataframe.columns, index=False)
    print ("Excel donusumu tamamlandi-----------station.xlsx")
    # csv dosyasini DXF'ye donustur
    export_DXF()
    # "open_output_folder" common.py'nin fonksiyonu. Output klasorunun acilmasi
    open_output_folder()