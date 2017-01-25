""" Script for harvesting metadata

AUTOR: Trygve Halsne, 25.01.2017

COMMENTS:
    - Implement it object oriented by means of classes
"""
# http://arcticdata.met.no/metamod/oai?verb=ListRecords&set=nmdc&metadataPrefix=dif
# Start by the tutorial from https://wiki.duraspace.org/display/ISLANDORA112/How+to+Harvest+Metadata+Records

import urllib2 as ul2
from xml.dom.minidom import parseString
import codecs

#baseURL =  'http://dalspace.library.dal.ca:8080/oai/request'
#arguments = '?verb=ListRecords&metadataPrefix=oai_dc'

#baseURL = 'http://union.ndltd.org/OAI-PMH/'
#getRecordsURL = str(baseURL+'?verb=ListRecords&metadataPrefix=oai_dc')

baseURL ='http://arcticdata.met.no/metamod/oai'
getRecordsURL = str(baseURL+'?verb=ListRecords&set=nmdc&metadataPrefix=dif')

file = ul2.urlopen(getRecordsURL)
data = file.read()
file.close()

dom = parseString(data)
data_xml = dom.toprettyxml()
response = codecs.open('test.xml','w','utf-8')
response.write(data_xml)
