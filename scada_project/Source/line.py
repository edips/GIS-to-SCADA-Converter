# -*- coding: utf8 -*-
# Author : Edip Ahmet TASKIN, Universal Yazilim A.Ş.
import numpy as np
from lib_line import *
from common import *

def line():
    # Dataframe'in olusturulmasi ve databaseden gerekli veri almasi
    p_dataframe = dataframe_from_database()
    # ITRF koordinatlarinin WGS84'e dönüşümü ve csv'ye kolon olarak eklenmesi
    wgs84_kolonlari_line(p_dataframe)
    # Veritabanindan alinan kolonlarin SCADA'ya gore yeniden adlandirilmasi
    rename_dataframe(p_dataframe)

    p_dataframe["NumVolt"] = p_dataframe["NumVolt"].astype(str)
    p_dataframe["NumVolt"] = desimal_sil(p_dataframe, "NumVolt")

    sabit_degerler(p_dataframe)
    p_dataframe['Insulation_type'] = p_dataframe['Insulation_type'].fillna(0).astype(int)

    # kva degerlerini NumVolt'dan sil
    p_dataframe["NumVolt"] = p_dataframe["NumVolt"].astype(str)
    p_dataframe["NumVolt"] = desimal_sil(p_dataframe, "NumVolt")

    # Değeri CCODE olan alanların lookup tablosundan description değerlerini çekmesi
    gis_to_scada(p_dataframe)
    p_dataframe["NumVolt"] = extract_numeric_value(p_dataframe, "NumVolt")
    p_dataframe['NumVolt'] = p_dataframe['NumVolt'].convert_objects(convert_numeric=True).apply(multiple)

    p_dataframe['GIS_ID'] = GIS_ID(p_dataframe)

    # Type kolonunda lookup tablosundan gelen HAVAİ yerine OHL, YERALTI yerine UG yazılması
    p_dataframe = p_dataframe.replace({'Type': {'HAVAİ': "OHL", 'YERALTI': "UG"}})
    # Conductor_Material kolonunda ALİMİNYUM yerine AL, BAKIR yerine CU yazılması
    p_dataframe.Conductor_Material = p_dataframe.Conductor_Material.apply(lambda x: 'AL' if 'AL' in x else x)
    p_dataframe.Conductor_Material = p_dataframe.Conductor_Material.apply(lambda x: 'CU' if 'BA' in x else x)
    # NULL'lari boş olarak yazdir:
    p_dataframe = p_dataframe.replace(np.nan, '')
    # p_dataframe.merge(p_dataframe, p_dataframe2, how='left', left_on='county_ID', right_on='countyid')
    p_dataframe = final_dataframe(p_dataframe)
    # csv'ye donusum

    p_dataframe.to_csv(r"..\Outputs\line.csv", sep=';', encoding='utf8', index=False)
    print ("CSV'ye donusum tamamlandi")

    # Excel'de maximum 1048000 kayit limiti oldugundan dataframe excel dosyalari olarak parcalara aydilir.
    #np.array_split(p_dataframe, 2)
    #df_splitted = np.array_split(p_dataframe, 2)
    #i = 0
    #for chunk in df_splitted:
    #    chunk.to_excel(r"..\Outputs\line_{:02d}.xlsx".format(i), index=False)
    #    print (r"..\Outputs\line_{:02d}.xlsx".format(i))
    #    i += 1
    #print "Excel'e donusum tamamlandi."