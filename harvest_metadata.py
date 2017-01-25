""" Script for harvesting metadata

AUTOR: Trygve Halsne, 25.01.2017

COMMENTS:
    - Implement it object oriented by means of classes
    - Send to github
"""

# List all recordsets: http://arcticdata.met.no/metamod/oai?verb=ListRecords&set=nmdc&metadataPrefix=dif
# List identifier: http://arcticdata.met.no/metamod/oai?verb=GetRecord&identifier=urn:x-wmo:md:no.met.arcticdata.test3::ADC_svim-oha-monthly&metadataPrefix=dif
import urllib2 as ul2
from xml.dom.minidom import parseString
import codecs
import sys

sys.path.insert(0,'/home/trygveh/documents/nbs/file_conversion/checkMMD')
from myCheckMMDFunctions import validKeywords


global valid_protocols
valid_protocols = ['OAI-PMH']

class HarvestMetadata(object):
    def __init__(self, baseURL, records, outputDir, hProtocol): # add outputname also
        """ set variables in class """
        self.baseURL = baseURL
        self.records = records
        self.outputDir = outputDir
        self.hProtocol = hProtocol

    #self.valid_protocols = ['OAI-PMH']

    def harvest(self):
        baseURL, records, hProtocol = self.baseURL, self.records, self.hProtocol
        if hProtocol == 'OAI-PMH':
            getRecordsURL = str(baseURL + records)
            harvestContent = ul2.urlopen(getRecordsURL)
            data = harvestContent.read()
            harvestContent.close()
            #print data
            #print '\n'
            #print parseString(data).toxml()
            output = codecs.open('test.xml','w')
            output.write(data)
            output.close()
        else:
            print 'Protocol %s is not accepted.' % hProtocol
            sys.exit()
        '''
        if not validKeywords([hProtocol],valid_protocols):
            print 'Protocol %s is not accepted.' % hProtocol
            sys.exit()
        '''


def main():
    hm = HarvestMetadata(baseURL='http://arcticdata.met.no/metamod/oai',
                          records='?verb=GetRecord&identifier=urn:x-wmo:md:no.met.arcticdata.test3::ADC_svim-oha-monthly&metadataPrefix=dif',
                          outputDir='tmp', hProtocol='OAI-PMH')
    hm.harvest()

if __name__ == '__main__':
    main()



"""
file = ul2.urlopen(getRecordsURL)
data = file.read()
file.close()

dom = parseString(data)
data_xml = dom.toprettyxml()
response = codecs.open('test.xml','w','utf-8')
response.write(data_xml)
"""

#baseURL ='http://arcticdata.met.no/metamod/oai'
#getRecordsURL = str(baseURL+'?verb=ListRecords&set=nmdc&metadataPrefix=dif')

#baseURL =  'http://dalspace.library.dal.ca:8080/oai/request'
#arguments = '?verb=ListRecords&metadataPrefix=oai_dc'

#baseURL = 'http://union.ndltd.org/OAI-PMH/'
#getRecordsURL = str(baseURL+'?verb=ListRecords&metadataPrefix=oai_dc')
