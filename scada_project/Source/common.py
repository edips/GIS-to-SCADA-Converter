# -*- coding: utf8 -*-
# Author : Edip Ahmet TASKIN, Universal Yazilim A.Ş.
import os
import errno
from configparser import ConfigParser
import pandas as pd
import sqlalchemy
import urllib
from pyproj import Proj
import arcpy

# Nokta icin wgs84 donusumu
def wgs84_kolonlari(p_dataframe,):
    myProj = Proj("+proj=tmerc +lat_0=0 +lon_0=30 +k=1 +x_0=500000 +y_0=0 +ellps=GRS80 +towgs84=0,0,0,0,0,0,0 +units=m +no_defs")
    lon, lat = myProj(p_dataframe['Point_X'].values, p_dataframe['Point_Y'].values, inverse=True)
    p_dataframe['Xwgs84'] = pd.Series(lon)
    p_dataframe['Ywgs84'] = pd.Series(lat)
# Line icin wgs84 donusumu
def wgs84_kolonlari_line(p_dataframe,):
    myProj = Proj("+proj=tmerc +lat_0=0 +lon_0=30 +k=1 +x_0=500000 +y_0=0 +ellps=GRS80 +towgs84=0,0,0,0,0,0,0 +units=m +no_defs")
    lon_start, lat_start = myProj(p_dataframe['Xstart'].values, p_dataframe['Ystart'].values, inverse=True)
    lon_end, lat_end = myProj(p_dataframe['Xend'].values, p_dataframe['Yend'].values, inverse=True)
    p_dataframe['Xwgs84_start'] = pd.Series(lon_start)
    p_dataframe['Ywgs84_start'] = pd.Series(lat_start)
    p_dataframe['Xwgs84_end'] = pd.Series(lon_end)
    p_dataframe['Ywgs84_end'] = pd.Series(lat_end)
# Config dosyasindan path tanimlama
def get_input():
    print ("")
    # GDB ve SDE veritabanlarinin dosya konumlari
    parser = ConfigParser()
    # GDB ve SDE veri tabanlarinin konumlari paths.ini dosyasindadir.
    #parser.read('.\Source\paths.ini')
    parser.read('paths.ini')
    k = parser.get('paths', 'sde_path')
    return k

# Fieldsleri listele, [field1, field2...]
def getFieldNames(shp):
    fieldnames = [f.name for f in arcpy.ListFields(shp)]
    return fieldnames

# CCODE iceren kolonlarin descriptionunu veri tabanindan cekme fonksiyonu.
def gis_description_to_scada(p_dataframe, feature_class, gis_column, scada_column):
    sql = (""" select distinct VW.CCODE, vw.CDESCRIPTION
    from %s t
    left join dbo.lookup_Code vw on t.%s=vw.CCODE where CCODE is not NULL
    """) % (feature_class, gis_column)

    # sql sorgusunu dataframe olarak aktar:
    df_gis_scada = pd.read_sql_query(sql, sqlalchemy_connection())
    # .0 desimal degerini sil
    df_gis_scada["CCODE"] = desimal_sil(df_gis_scada,"CCODE")

    p_dataframe[scada_column] = p_dataframe[scada_column].fillna('')
    p_dataframe[scada_column] = p_dataframe[scada_column].astype(str)

    p_dataframe[scada_column] = desimal_sil(p_dataframe,scada_column)
    p_dataframe[scada_column] = join_column_from_dataframe_to_dataframe(p_dataframe,df_gis_scada,scada_column,"CCODE","CDESCRIPTION")
    p_dataframe[scada_column] = p_dataframe[scada_column].fillna('')
    # trafo icin
    if scada_column == 'Winding_Connection_Type':
        p_dataframe.loc[p_dataframe['Winding_Connection_Type'].str.contains('YZN'), 'Winding_Connection_Type'] = '4'
        p_dataframe.loc[p_dataframe['Winding_Connection_Type'].str.contains('DYN'), 'Winding_Connection_Type'] = '9'
    # line icin
    elif scada_column == 'Insulation_type':
        p_dataframe.loc[~p_dataframe['Insulation_type'].astype(str).str.contains('YERALTI', na=False), 'Insulation_type'] = ''
        p_dataframe.loc[p_dataframe['Insulation_type'].str.contains('YERALTI'), 'Insulation_type'] = 'PTF'

# Empendans tablosunun power ve voltage kolonlarina bagli olarak degisen empedans degerlerinin donusunu icin bu fonksiyon kullanilir.
# Ayrica iki kolona bagli kolonlarin donusumu icinde kullanilabilir.
def iki_kolonu_tuple_olarak_birlestir(dataframe,kolon, kolon1,kolon2):
    dataframe[kolon] = list(zip(dataframe[kolon1], dataframe[kolon2]))
    return dataframe[kolon].astype(str)

# iki kolonu liste olarak birlestirir, empedans kolonu icin kulanilir
def iki_kolonu_list_olarak_birlestir(dataframe, ana_kolon, column1, column2):
    dataframe[ana_kolon] = list(zip(dataframe[column1], dataframe[column2]))
    return dataframe[ana_kolon].astype(str)

# dataframe serisinde key value degerlerinin yer degistirilmesinde kullanilir. Mesela bir seri CCODE iceriyorsa description olan
# dataframe CCODE ile uyusuyorsa yerdegistirir.
def join_column_from_dataframe_to_dataframe(dataframe1,dataframe2,degistirilecek_kolon,ortak_kolon,yeni_kolon):
    return dataframe1[degistirilecek_kolon].map(dataframe2.set_index(ortak_kolon)[yeni_kolon]).fillna('')

def kolondaki_unicodu_sil(dataframe, column):
    series = dataframe[column]
    return [s.encode('ascii', 'ignore').strip() for s in series.str.decode('unicode_escape')]

def isnul_if_parentColumn_isnull(p_dataframe,parentColumn, childColumn):
    p_dataframe.loc[p_dataframe[parentColumn].isnull(), childColumn] = p_dataframe[parentColumn]
    return p_dataframe

# kolondaki bosluklari sil
def kolondaki_bosluklari_sil(dataframe, kolon):
    return dataframe[kolon].str.replace(' ', '')

# .0 desimal degerini silme fonksiyonu
def desimal_sil(dataframe, kolon):
    return dataframe[kolon].astype(str).replace('\.0', '', regex=True)

# kolonda birimleriyle beraber verilen sayisal degerlerin numerik kismini cikaran fonksiyon
def extract_numeric_value( dataframe, kolon ):
    dataframe.loc[dataframe[kolon].str.contains(r"[a-zA-Z]", case=False), kolon] = dataframe[kolon].astype(str).str.extract('(\d+(?:\.\d+)?)', expand=False)
    return dataframe[kolon]

# SqlAlchemy baglantisinin yapilmasi
def sqlalchemy_connection():
    # connection parameters
    user = 'sa'
    password = 'sa.123'
    database = 'SCADA'
    server = '10.0.1.228'
    driver = '{SQL Server}'
    con_string = 'UID=%s;PWD=%s;DATABASE=%s;SERVER=%s;driver=%s' % (user, password, database, server, driver)
    params = urllib.quote_plus(con_string)
    # python 3 icin:
    # params = urllib.parse.quote_plus(con_string)
    my_db = sqlalchemy.create_engine('mssql+pyodbc:///?odbc_connect=%s' % params)
    return my_db

# read csv
def read_csv(csv_path):
    return pd.read_csv(csv_path, index_col=False, encoding='utf-8')

# dosya silme
def dosya_sil(filename):
    try:
        os.remove(filename)
    except OSError as e:
        if e.errno != errno.ENOENT:
            raise

# islem bittikten sonra output klasoru acilir
def open_output_folder():
    path = "..\\Outputs"
    #path = ".\\Outputs"
    path = os.path.realpath(path)
    os.startfile(path)

# dataframe serisinde yer degistirmek icin kullanilir.
def yerdegistir(p_dataframe,kolon,eski_deger,yeni_deger):
    return p_dataframe[kolon].replace(eski_deger, yeni_deger)

def klasor_olustur(dxf_folder):
    if not os.path.exists(dxf_folder):
        os.makedirs(dxf_folder)
# klasorde dosya adi benzer olan dosyalari filitreler ve siler
def delete_files_startwith(my_dir, start_with):
    for fname in os.listdir(my_dir):
        if fname.startswith(start_with):
            os.remove(os.path.join(my_dir, fname))


def number2string_deleteDecimal(dataframe,kolon):
    dataframe[kolon] = dataframe[kolon].fillna('')
    dataframe[kolon] = dataframe[kolon].astype(str)
    dataframe[kolon] = desimal_sil(dataframe, kolon)
    return dataframe[kolon]