#!/usr/bin/env python
#-*- coding: utf-8 -*-
# Author : Edip Ahmet TASKIN, Universal Yazilim A.Ş.
# encoding icin reload(sys) kullanilir
import os
from site import addsitedir
from sys import executable
from os import path
from common import *
interpreter = executable
sitepkg = path.dirname(interpreter) + "\\site-packages"
addsitedir(sitepkg)


print ("")
print ("  transformator icin -----------> 1 ")
print ("  anahtarlama elemanlari icin --> 2 ")
print ("  hat icin ---------------------> 3 ")
print ("  bara icin --------------------> 4 ")
print ("  station icin -----------------> 5 ")
print ("  generator icin ---------------> 6 ")
print ("  load icin --------------------> 7 ")
print ("")
print(" CSV ve Excel ciktisi alinacak katmanin numarasini girip Enter'e basiniz. \n" +
      "Katmanlarin hepsinin ciktilarini almak icin sadece Enter'e basiniz:")

print ("SDE konumu: ", get_input() )
# output path
directory = "..\\Outputs"
#directory = ".\\Outputs"
if not os.path.exists(directory):
    os.makedirs(directory)
secim = raw_input(">> ")
if secim == "1":
    import transformer
    transformer.transformer()
    print ("")
    print ("Donusum islemi tamamlandi. ")
elif secim == "2":
    import anahtar
    anahtar.switch()
    print ("")
    print ("Donusum islemi tamamlandi. ")
elif secim == "3":
    import line
    line.line()
    print""
    print ("Donusum islemi tamamlandi. ")
elif secim == "4":
    import busbar
    # busbar.busbar()
    busbar.connector()
    print ("")
    print ("Donusum islemi tamamlandi. ")
elif secim == "5":
    import station
    station.station()
    print ("")
    print ("Donusum islemi tamamlandi. ")
elif secim == "6":
    import generator
    generator.generator()
    print ("")
    print ("Donusum islemi tamamlandi. ")
elif secim == "7":
    import load
    load.load()
    print ("")
    print ("Donusum islemi tamamlandi. ")
elif secim == "":
    import station
    station.station()
    import transformer
    transformer.transformer()
    import anahtar
    anahtar.switch()
    import line
    line.line()
    import busbar
    busbar.connector()
    import generator
    generator.generator()
    print ("")
    print ("Donusum islemi tamamlandi. ")
else:
    print ("Hatali giris, tekrar deneyiniz:")
