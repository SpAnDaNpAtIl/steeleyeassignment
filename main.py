import requests as re
import zipfile #https://stackoverflow.com/questions/3451111/unzipping-files-in-python
import os
from bs4 import BeautifulSoup
import pandas as pd
import logging

# Create and configure logger
logging.basicConfig(filename="logs.log",
                    format='%(asctime)s %(message)s',
                    filemode='w')
logger = logging.getLogger()
logger.setLevel(logging.INFO)

logger.info('Starting code...')
logger.info('Accessing link')
res = re.get(url='https://registers.esma.europa.eu/solr/esma_registers_firds_files/select?q=*&fq=publication_date'
                 +':%5B2021-01-17T00:00:00Z+TO+2021-01-19T23:59:59Z%5D&wt=xml&indent=true&start=0&rows=100')

res_object = BeautifulSoup(res.text, features="xml")
first_doc = res_object.find('result').find('doc')
#checking if file_type is 'DLTINS'
if(first_doc.find('str', attrs={'name': 'file_type'}).text == 'DLTINS'):
    logger.info('Link with file_type DLTINS found')
    first_link = first_doc.find('str', attrs={'name':'download_link'}).text
    first_link_name = first_doc.find('str', attrs={'name':'file_name'}).text
else:
    logger.warning('No link with file_type DLTINS found')

#downloading the file
logger.info('Downloading file')
res = re.get(url=first_link)
with open(first_link_name, 'wb') as f:
    f.write(res.content)


logger.info('Unzipping file')
with zipfile.ZipFile(first_link_name,"r") as zip_ref:
    zip_ref.extractall("zip_contents")

file_name = os.listdir("zip_contents")[0]
with open("zip_contents/"+file_name, 'r', encoding='utf8') as f:
    logger.info('Parsing xml file')
    file_data = f.read()

file_data = BeautifulSoup(file_data, features="xml")
id=[]
FullNm=[]
ClssfctnTp=[]
CmmdtyDerivInd=[]
NtnlCcy=[]
Issr=[]
logger.info('Reading rows from xml file')
for i in file_data.find('Document').find('FinInstrmRptgRefDataDltaRpt').find_all('FinInstrm'):
    attributes = i.find('FinInstrmGnlAttrbts')
    id.append(attributes.find('Id').text)
    FullNm.append(attributes.find('FullNm').text)
    ClssfctnTp.append(attributes.find('ClssfctnTp').text)
    CmmdtyDerivInd.append(True if attributes.find('CmmdtyDerivInd').text=='true' else False) #bool
    NtnlCcy.append(attributes.find('NtnlCcy').text)
    issr = i.find('Issr')
    Issr.append(issr.text)

df = pd.DataFrame({'FinInstrmGnlAttrbts.Id':id, 'FinInstrmGnlAttrbts.FullNm':FullNm, 'FinInstrmGnlAttrbts.ClssfctnTp':ClssfctnTp, 'FinInstrmGnlAttrbts.CmmdtyDerivInd':CmmdtyDerivInd, 'FinInstrmGnlAttrbts.NtnlCcy':NtnlCcy, 'Issr':Issr})
logger.info('Saving to csv file')
df.to_csv('output.csv', index=False)
logger.info('csv file saved as output.csv')


