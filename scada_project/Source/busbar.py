# -*- coding: utf8 -*-
# Author : Edip Ahmet TASKIN, Universal Yazilim A.Ş.
from lib_busbar import *
import csv

def connector():
    # veritabanindan dataframe olustur.
    p_dataframe = dataframe_from_database()
    wgs84_kolonlari_line(p_dataframe)
    # olusan dataframe'yi yeniden adlandir.
    rename_dataframe(p_dataframe)
    # Nominal_Voltage kolonundan desimali sil
    desimal_sil(p_dataframe, "Nominal_Voltage")
    # sabit degerleri ata
    sabit_degerler(p_dataframe)
    # Facility_ID kolonuna Substation kolonunun kumulatif degerleri atanir
    facility_id(p_dataframe)
    # substation kolonunu stringe cevir
    p_dataframe["Substation"] = p_dataframe["Substation"].astype(str)
    # Substationda .0 desimal degerini sil
    p_dataframe["Substation"] = desimal_sil(p_dataframe, "Substation")
    # NULL degerleri bosluk yap
    p_dataframe = p_dataframe.fillna('')
    # stringe cevrilmis olan null'lar yerine nan gelir, nanlari bosluk yap
    p_dataframe = p_dataframe.replace('nan', '', regex=True)
    # B3_Text kolonu Facility_ID'ye baglidir. Facility ID 1 ise BB1A, 2 ise BB2A gelir,
    # 2'den fazla olan degerlerde B3_Text Facility_ID ile ayni degerleri alir.
    # p_dataframe['B3_Text'] = B3_Text(p_dataframe)
    # Facility_ID numerik oldugunda NULL'lar 0 olur, bunu onlemek icin stringe cevrilir.
    p_dataframe["Facility_ID"] = p_dataframe["Facility_ID"].astype(str)
    # Substation'a bagli olan kolonlardan Facility_ID ve B3_Text kolonlari icin
    # Substation bos oldugunda  bu kolonlarinda bos gelmesi islemi
    p_dataframe.Facility_ID[p_dataframe.Substation == ""] = p_dataframe.Substation
    # p_dataframe.B3_Text[p_dataframe.Substation == ""] = p_dataframe.Substation
    # BARA_ASSETID
    p_dataframe['BARA_ASSETID'] = p_dataframe['Substation'].astype(str) + '0' + p_dataframe['Facility_ID'].astype(str)
    # GIS_ID
    p_dataframe['GIS_ID'] = "Busbar000000" + p_dataframe['ASSETID'].astype(str)
    # Substation'a bagli olan BARA_ASSETID kolonu icin Substation bos oldugunda  bu kolon bos gelmelidir
    p_dataframe.BARA_ASSETID[p_dataframe.Substation == ""] = p_dataframe.Substation
    # Normal_Voltage'nin CCODE yerine OPERATIONAL_VOLTAGE'nin descriptionunu cekmesi
    gis_description_to_scada(p_dataframe, "ELE_DD_CONNECTOR_evw", "OPERATING_VOLTAGE", 'Nominal_Voltage')
    # Dataframe'nin son hali, kolonlari yerlestirilmesi
    p_dataframe = final_dataframe(p_dataframe)

    start_x = list(p_dataframe["Xstart"])
    start_y = list(p_dataframe["Ystart"])
    end_x = list(p_dataframe["Xend"])
    end_y = list(p_dataframe["Yend"])

    # csv ve excel'e export islemi

    # --------------------------temporary test script --------------------------------------------
    #print p_dataframe
    #p_dataframe = p_dataframe[p_dataframe.OBJECTID < 200]
    #p_dataframe.to_csv(r"..\Outputs\busbar_test.csv", sep=';', encoding='utf8', index=False)
    #print ("Test icin CSV'ye donusum tamamlandi")


    # ---------------------------------------------------------------------
    dataframe_export(p_dataframe)
    # islem bittiginde output klasoru acilir
    open_output_folder()