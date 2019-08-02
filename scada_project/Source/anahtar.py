# -*- coding: utf8 -*-
# Author : Edip Ahmet TASKIN, Universal Yazilim A.Ş.
import numpy as np
from lib_anahtar import *

def switch():
    # Dataframe'in olusturulmasi ve databaseden gerekli veri almasi
    p_dataframe = dataframe_from_database()
    # ITRF koordinatlarinin WGS84'e dönüşümü ve csv'ye kolon olarak eklenmesi
    wgs84_kolonlari(p_dataframe)
    # Kolonların yeniden adlandırılması
    rename_dataframe(p_dataframe)
    # Değeri CCODE olan alanların lookup tablosundan description değerlerini çekmesi
    gis_to_scada(p_dataframe)
    # sabit degerlerin atanmasi:
    sabit_degerler(p_dataframe)
    # Nominal_Voltage alanının nümerik değerlerinin çıkarılması
    p_dataframe["Nominal_Voltage"] = extract_numeric_value(p_dataframe, "Nominal_Voltage")
    # NULL değerlerin boşluk olarak aktarılması
    p_dataframe = p_dataframe.replace(np.nan, '')
    # Kolonların csv'de ki son hali
    p_dataframe = final_dataframe(p_dataframe)
    # csv'ye donusum
    p_dataframe.to_csv(r"..\Outputs\switch.csv", sep=';', encoding='utf-8', index=False)
    print "csv'ye donusum tamamlandi."
    # Excel'e donusum
    p_dataframe.to_excel(r"..\Outputs\switch.xlsx", 'Sheet1', p_dataframe.columns, index=False)
    print "Excel'e donusum tamamlandi."
    # DXF donusumu
    csv_path = ("..\\Outputs\\switch.csv")
    export_DXF(csv_path)
    # output klasorunu ac
    open_output_folder()