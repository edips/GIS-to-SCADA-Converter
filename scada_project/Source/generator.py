# -*- coding: utf8 -*-
# Author : Edip Ahmet TASKIN, Universal Yazilim A.Ş.
from lib_generator import *
from common import *
def generator():
    # Dataframe'in olusturulmasi ve databaseden gerekli veri almasi
    p_dataframe = dataframe_from_database()

    wgs84_kolonlari(p_dataframe)

    # Alan degeri description olan alanlarin fonksiyon olarak calistirilmasi:
    gis_description_to_scada(p_dataframe,"ELE_GENERATOR_EVW","GENERATOR_BRANDNAME","B3_Text")

    # Sabit degerlerli kolonlarin olusturulmasi
    dataframeye_sabit_degerleri_ata(p_dataframe)
    # Substation bos oldugunda  bu kolonlarinda bos gelmesi islemi
    facility_id_query(p_dataframe)
    # p_dataframe.Facility_ID[p_dataframe.Substation == ""] = p_dataframe.Substation
    # Alanlarin siralanmasi
    p_dataframe = final_dataframe(p_dataframe)
    # Dataframe'nin csv'ye donusumu
    p_dataframe.to_csv(r"..\Outputs\generator.csv", sep=';', encoding='utf-8', na_rep="", index=False)
    # p_dataframe.to_csv(r".\Outputs\transformar.csv", sep=';', encoding='utf-8', na_rep="", index=False)
    print ("CSV donusumu tamamlandi-----------generator.csv")
    # Dataframe'nin excele donusumu
    p_dataframe.to_excel(r"..\Outputs\generator.xlsx", 'Sheet1', p_dataframe.columns, index=False)
    # p_dataframe.to_excel(r".\Outputs\transformer.xlsx", 'Sheet1', p_dataframe.columns, index=False)
    print ("Excel donusumu tamamlandi-----------generator.xlsx")
    # csv dosyasini DXF'ye donustur
    export_DXF()
    # "open_output_folder" common.py'nin fonksiyonu. Output klasorunun acilmasi
    open_output_folder()