""" Script for harvesting metadata
    Inspired by harvest-metadata from https://github.com/steingod/mdharvest/tree/master/src
    and code from http://lightonphiri.org/blog/metadata-harvesting-via-oai-pmh-using-python

AUTOR: Trygve Halsne, 25.01.2017

USAGE:
    - input must have metadataPrefix?

COMMENTS:
    - Implement it object oriented by means of classes
    - Send to github
"""

# List all recordsets: http://arcticdata.met.no/metamod/oai?verb=ListRecords&set=nmdc&metadataPrefix=dif
# List identifier: http://arcticdata.met.no/metamod/oai?verb=GetRecord&identifier=urn:x-wmo:md:no.met.arcticdata.test3::ADC_svim-oha-monthly&metadataPrefix=dif
# Recordset with resumptionToken: http://union.ndltd.org/OAI-PMH/?verb=ListRecords&metadataPrefix=oai_dc
# Recordset with DIF elements and resumptionToken (NB! Slow server..): http://ws.pangaea.de/oai/provider?verb=ListRecords&metadataPrefix=dif
# Recordset with DIF elements and resumptionToken: https://esg.prototype.ucar.edu/oai/repository.htm?verb=ListRecords&metadataPrefix=dif

import urllib2 as ul2
import urllib as ul
from xml.dom.minidom import parseString
import codecs
import sys

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
            print "Harvesting metadata \n"
            getRecordsURL = str(baseURL + records)

            # Initial phase
            resumptionToken = self.oaipmh_resumptionToken(getRecordsURL)
            dom = self.oaipmh_harvestContent(getRecordsURL)
            if dom != None:
                self.oaipmh_writeDIFtoFile(dom)
            pageCounter = 1

            while resumptionToken != []:
                #print "\nHandeling resumptionToken: " + str(pageCounter)
                resumptionToken = ul.urlencode({'resumptionToken':resumptionToken}) # create resumptionToken URL parameter
                print resumptionToken
                getRecordsURLLoop = str(baseURL+'?verb=ListRecords&'+resumptionToken)
                dom = self.oaipmh_harvestContent(getRecordsURLLoop)
                if dom != None:
                    self.oaipmh_writeDIFtoFile(dom)
                else:
                    print "dom = " + str(dom) + ', for page ' + str(pageCounter)

                resumptionToken = self.oaipmh_resumptionToken(getRecordsURLLoop)
                pageCounter += 1

            return dom

        else:
            print 'Protocol %s is not accepted.' % hProtocol
            sys.exit()

    def oaipmh_writeDIFtoFile(self,dom):
        """ Write DIF elements in dom to file """
        print "Writing DIF elements to disk"

        record_elements = dom.getElementsByTagName('record')
        size_dif = dom.getElementsByTagName('DIF').length

        if size_dif != 0:
            counter = 0

            for record in record_elements:
                for child in record.childNodes:
                    if str(child.nodeName) == 'header':
                        has_attrib = child.hasAttributes()
                        for gchild in child.childNodes:
                            if gchild.nodeName == 'identifier':
                                id_text = gchild.childNodes[0].nodeValue
                                break;

                if not has_attrib:
                    sys.stdout.write('Extracting %.f / %d DIF elements \r' %(counter,size_dif))
                    sys.stdout.flush()
                    dif = record.getElementsByTagName('DIF')[0]
                    #tmp_fname ='dif_test_' + str(id_text) + '.xml'
                    tmp_fname ='dif_test_' + str(counter) + '.xml'
                    output = codecs.open(tmp_fname ,'w','utf-8')
                    dif.writexml(output)
                    output.close()
                    counter += 1
                if counter == 2:
                    break;
        else:
            print "records did not contain DIF elements"

    def oaipmh_harvestContent(self,URL):
        try:
            file = ul2.urlopen(URL,timeout=40)
            data = file.read()
            file.close()
            return parseString(data)
        except ul2.HTTPError:
            print "There was an error with the request"

    def oaipmh_resumptionToken(self,URL):
        try:
            file = ul2.urlopen(URL, timeout=40)
            data = file.read()
            file.close()
            dom = parseString(data)

            if dom.getElementsByTagName('resumptionToken').length == 0:
                return dom.getElementsByTagName('resumptionToken')
            else:
                if dom.getElementsByTagName('resumptionToken')[0].firstChild != None:
                    return dom.getElementsByTagName('resumptionToken')[0].firstChild.nodeValue
                else:
                    return []
        except ul2.HTTPError:
            print "There was an error with the request"


baseURL = 'https://esg.prototype.ucar.edu/oai/repository.htm'
records = '?verb=ListRecords&metadataPrefix=dif'
outputDir = 'tmp'
hProtocol = 'OAI-PMH'

hm = HarvestMetadata(baseURL,records, outputDir, hProtocol)
content = hm.harvest()

'''
hm = HarvestMetadata(baseURL='http://arcticdata.met.no/metamod/oai',
                      records='?verb=ListRecords&set=nmdc&metadataPrefix=dif',
                      outputDir='tmp', hProtocol='OAI-PMH')
content = hm.harvest()

'''

"""
def main():
    hm = HarvestMetadata(baseURL='http://arcticdata.met.no/metamod/oai',
                          records='?verb=GetRecord&identifier=urn:x-wmo:md:no.met.arcticdata.test3::ADC_svim-oha-monthly&metadataPrefix=dif',
                          outputDir='tmp', hProtocol='OAI-PMH')
    content = hm.harvest()
    print content
if __name__ == '__main__':
    main()
"""


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
