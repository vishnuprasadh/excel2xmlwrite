import pandas as pd
import numpy as np
import xml.etree.cElementTree as xtree
from xml.etree import ElementTree
import logging
import logging.handlers
from logging.handlers import RotatingFileHandler
import os
from datetime import datetime

class processproducts:
    logger = logging.getLogger("processproducts")
    logger.addHandler(RotatingFileHandler("producessproducts.log",'a',100000,2))
    logger.setLevel(logging.INFO)
    dfassort = pd.DataFrame()
    xmldoc = []
    _CLUSTER="cluster"
    _ARTICLECODE ="Article Code"
    _BRICKCODE ="Brick code"
    _WEIGHTEDITEM = "weightedItem"
    _FINITEINVENTORY ="finiteinventory"
    _DESCRIPTION ="Desc"
    _RRP = "RRP"
    _MRP1 = "MRP1"
    _MRP2 = "MRP2"
    _MRP3 = "MRP3"
    _MRP4 = "MRP4"
    _MRP5 = "MRP5"
    _FOODCOUPON ="foodcouponaccepted"
    _BRICKCODE = "Brick code"
    _BRICK = "Brick"
    _UOM ="UOM"
    _FAMILYCODE = "Family code"
    _MCDESC ="MC. Desc"
    _CLASSCODE ="Class code"
    _SEGMENTCODE = "segment code"
    _L1 = "L1"
    _L2="L2"
    _L3="L3"
    _SPECIALITY ="speciality"

    def __init__(self):
        self.dfassort = pd.read_csv(os.path.join(os.path.dirname(__file__),"assortment.csv"))


    def process(self):
        try:
            item = self._createroot('item')

            index = 1
            for index,row in self.dfassort.iterrows():
                try:
                    description = str(row[self._DESCRIPTION]).strip().encode()
                    cluster = self._startelement("cluster",item)
                    self._startelement("clusterid",cluster, str(row[self._CLUSTER]))
                    product = self._startelement("product",item)
                    self._startelement("code",product, "G{}".format(row[self._ARTICLECODE]))
                    self._startelement("articleNumber",product,str(row[self._ARTICLECODE]))
                    self._startelement("name",product,description)
                    self._startelement("shortdesc",product,description)
                    self._startelement("longdesc", product,description)
                    self._startelement("uom", product,str(row[self._UOM]))

                    weighteditem = str(row[self._WEIGHTEDITEM]).lower()
                    foodcoupon= str(row[self._FOODCOUPON]).lower()
                    speciality=str(row[self._SPECIALITY]).lower()
                    self._startelement("weighteditem", product,weighteditem)
                    self._startelement("foodcouponexpected", product,foodcoupon)
                    self._startelement("speciality", product, speciality)

                    category = self._startelement("category", item)
                    self._startelement("segmentcode", category,str(row[self._SEGMENTCODE]))
                    self._startelement("familycode", category, str(row[self._FAMILYCODE]))
                    self._startelement("classcode", category,str(row[self._CLASSCODE]))
                    self._startelement("brickname", category,str(row[self._BRICK]))
                    self._startelement("brickcode", category,str(row[self._BRICKCODE]))

                    price = self._startelement("price", item)
                    self._startelement("rrp", price,str(row[self._RRP]))
                    self._startelement("mrp1", price,str(row[self._MRP1]))
                    self._startelement("mrp2", price,str(row[self._MRP2]))
                    self._startelement("mrp3", price,str(row[self._MRP3]))
                    self._startelement("mrp4", price,str(row[self._MRP4]))
                    self._startelement("mrp5", price,str(row[self._MRP5]))

                    brand = self._startelement("brand", item)
                    brandname = ""
                    if description.split(" "):
                        brandname= description.split(" ")[0]
                    else:
                        brandname=description
                    self._startelement("brandname", brand,brandname  )
                    self._startelement("brandtypelabel", brand, brandname)
                    self._startelement("enrichedbrandname", brand,description)
                    self._startelement("brandcode", brand,"B_{}".format(brandname,row[self._ARTICLECODE]))
                except Exception as ex:
                    self.logger.error("Failed to process for row {} - exception - {}".format(index,ex))
                finally:
                    index = index + 1
                    self.logger.info("Processed row {} successfully.".format(index))

            root = xtree.ElementTree(item)
            root.write("output.xml")
        except Exception as ex:
            self.logger.error("Failed write or creation of root node due to - {}".format( ex))
        finally:
            self.logger.info("Completed processing at {}".format( datetime.now().strftime('%d-%m-%Y %H:%M:%S')))

    def _createroot(self,rootname):
        root = xtree.Element(rootname)
        root.set("xmlns:xsi","http://www.w3.org/2001/XMLSchema-instance")
        root.set("xsi:noNamespaceSchemaLocation","product-hybris-v2.0.xsd")
        return root


    def _startelement(self,elementname, parentelement, elementvalue=None, attributelist=None):
        child = xtree.SubElement(parentelement, elementname)
        if elementvalue:
            child.text =elementvalue
        if attributelist:
            self._addattributes(child,attributelist)
        return child


    def _addattributes(self,element, attributelist):
        for attribute in attributelist:
            element.set(attribute,attributelist[attribute])


    def _attribute(self,element, name, value):
        xtree.SubElement(element)



if __name__ == "__main__":
    prod = processproducts()
    prod.process()