# -*- coding: utf8 -*-
# Author : Edip Ahmet TASKIN, Universal Yazilim A.Ş.
from lib_transformer import *
def transformer():
    # Dataframe'in olusturulmasi ve databaseden gerekli veri almasi
    p_dataframe = dataframe_from_database()
    # ITRF koordinatlarinin WGS84'e dönüşümü ve csv'ye kolon olarak eklenmesi
    wgs84_kolonlari(p_dataframe)
    # Veritabanindan alinan kolonlarin SCADA'ya gore yeniden adlandirilmasi
    p_dataframe = rename_dataframe(p_dataframe)
    # Değeri CCODE olan alanların lookup tablosundan description değerlerini çekmesi
    p_dataframe = gis_description_to_scada2(p_dataframe)
    # Empedans tablosuna (Transformer_Impedance_Referance.csv) bagli olan kolonların aktarım işlemi
    p_dataframe = empedans_read_from_csv(p_dataframe)
    # Sabit degerlerli kolonlarin olusturulmasi
    sabit_degerler(p_dataframe)
    # Facility_ID istasyona bağlı olduğundan substation null ise facility_id de null gelmeli
    p_dataframe["Facility_ID"] = p_dataframe["Facility_ID"].astype(str)
    # İstasyona bagli olan kolonlardan Facility_ID ve B3_Text kolonlari icin
    # Substation bos oldugunda  bu kolonlarinda bos gelmesi islemi
    p_dataframe.Facility_ID[p_dataframe.Substation == ""] = p_dataframe.Substation
    # Kolonların csv'de sıralanması
    p_dataframe = final_dataframe(p_dataframe)
    # Dataframe'nin csv'ye donusumu
    p_dataframe.to_csv(r"..\Outputs\transformar.csv", sep=';', encoding='utf-8', na_rep="", index=False)
    # p_dataframe.to_csv(r".\Outputs\transformar.csv", sep=';', encoding='utf-8', na_rep="", index=False)
    print ("CSV donusumu tamamlandi-----------transformer.csv")
    # Dataframe'nin excele donusumu (Excel'e dönüşüm şartı yok.
    #  Bu dönüşüm kontrolde kolaylık sağlamak için yapılmaktadır.)
    p_dataframe.to_excel(r"..\Outputs\transformer.xlsx", 'Sheet1', p_dataframe.columns, index=False)
    # p_dataframe.to_excel(r".\Outputs\transformer.xlsx", 'Sheet1', p_dataframe.columns, index=False)
    print ("Excel donusumu tamamlandi-----------transformer.xlsx")
    # csv dosyasini DXF'ye donustur
    export_DXF()
    # "open_output_folder" common.py'nin fonksiyonu. Output klasorunun acilmasi
    open_output_folder()