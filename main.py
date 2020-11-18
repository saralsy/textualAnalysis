'''
10-K Cleaning: 2005-2020 Data 
Modified the following code: https://gist.github.com/anshoomehra/ead8925ea291e233a5aa2dcaa2dc61b2

Nick Zarra (2020-06-09)
Generalized in Augst for All Firms
'''
###############################################################################
# 0A) Set working directory and packages
###############################################################################
import os
import re
import numpy as np
import pandas as pd
import json
from natsort import natsorted, ns
import datetime
import time
from bs4 import BeautifulSoup


# \\ for windows, // for mac...sets the proper path
windowsOrMac = "/"
# dropboxpathWQ
dropboxpath = "/Sample_data/320193/10-K/000"
raw_data_path = 'D:/NYSE'

os.chdir(dropboxpath)


###############################################################################

# 1) LOAD CODE LIST
###############################################################################
'''
file_path = "C:/Users/nzarra/Dropbox/TextualAssetPricing/Data/Raw_Data/CompanyLists/"
name = "NYSE_Missing_CIK_Handv2.xlsx"
fp = file_path+name
NYSEMissing = pd.read_excel(fp)

CIK_list = list(NYSEMissing['cik'].astype(str))
CIK_list_unique = set(CIK_list)
'''
file_path = "/320193/10-K"
name = "Apple_2017.txt"
fp = file_path+name
NYSEFound = pd.read_excel(fp)

CIK_list = list(NYSEFound['cik'].sort_values())
CIK_list_unique = list(set(CIK_list))
CIK_list_unique=list(CIK_list_unique)
CIK_list_unique.sort()


###############################################################################
# .5 Create Status Holder
###############################################################################



Status_Holder  =  pd.DataFrame(columns  =  ['CIK','Q_CIK','Q_LastDate','file_name','Status'])
###############################################################################
# 1) Clean and Save
###############################################################################

def edits1(wordd):
    alphabet = ' '
    def splits(wordd):
        return [(wordd[:i], wordd[i:]) for i in range(len(wordd)+1)]
    pairs = splits(wordd)
    inserts = [a+c+b for (a, b) in pairs for c in alphabet]
    inserts_new=[x.strip() for x in inserts]
    return list(set(inserts_new))

def edits2(wordd):
    return list({e2 for e1 in edits1(wordd) for e2 in edits1(e1)})

def edits3(wordd):
    return list({e2 for e1 in edits1(wordd) for e2 in edits2(e1)})
def edits4(wordd):
    return list({e2 for e1 in edits1(wordd) for e2 in edits3(e1)})
def edits5(wordd):
    return list({e2 for e1 in edits1(wordd) for e2 in edits3(e1)})
def edits6(wordd):
    return list({e2 for e1 in edits1(wordd) for e2 in edits3(e1)})
def edits7(wordd):
    return list({e2 for e1 in edits1(wordd) for e2 in edits3(e1)})
def edit8(wordd):
    return list({e2 for e1 in edits1(wordd) for e2 in edits3(e1)})


item_1="|".join(edits1('item')+edits2('item')+edits3('item')+edits4('item'))
risk_1="|".join(edits1('risk')+edits2('risk')+edits3('risk')+edits4('risk'))
business_1="|".join(edits1('business')+edits2('business')+edits3('business')+edits4('business')+edits5('business')+edits6('business')+edits7('business'))
properties_1="|".join(edits1('properties')+edits2('properties')+edits3('properties')+edits4('properties')+edits5('properties')+edits6('properties')+edits7('properties'))

#clist = CIK_list_unique[100:300]
clist = list(CIK_list_unique)
no10k = []
emptyfolder = []
pre2006 = []
#clist=[1467027]
#clist=[713676]
'''
CIK_list_unique=list(set(pd.read_excel('C:/Users/nzarra/Dropbox/TextualAssetPricing/Data/Intermediate_data/FixMissingFile/missing_CIK_codes_noFIRE_domestic_missingfromlist.xls').cik.to_list()))
CIK_list_missing=list(set(pd.read_excel('C:/Users/nzarra/Dropbox/TextualAssetPricing/final_update/no_CIK_try2.xlsx').cik.to_list()))
clist=list(set(CIK_list_unique)-set(CIK_list_missing))

'''


CIK_list_unique=list(set(pd.read_excel('C:/Users/nzarra/Dropbox/TextualAssetPricing/Data/Intermediate_data/FixMissingFile/CIK_failed.xls').cik.to_list()))
clist=list(set(CIK_list_unique))

clist=[85961]
for CIK in clist:
    #time.sleep(.25)
    #datapath = windowsOrMac.join([raw_data_path,"raw_data","10-K", 'NYSE', "sec_edgar_filings",str(CIK),"10-K"])
    datapath = windowsOrMac.join([raw_data_path, "sec_edgar_filings",str(CIK),"10-K"])
    
    try:
        folder_size = len(os.listdir(datapath))
        counter_no10K = folder_size
        os.listdir(datapath)
        for index in range(0,len(os.listdir(datapath))):   
            Status  =  pd.DataFrame(np.zeros([1,5]),columns  =  ['CIK','Q_CIK','Q_LastDate','file_name','Status'])
            x  =  dict( Year  =  1900,CIK_Search = 0,CIK_File = "",etc = "",LastDate = '',SectionNew = "SectionNew",SectionOld = "SectionOld")
    
            intermediatepath = windowsOrMac.join([dropboxpath,"intermediate_data", 'NYSE', "sec_edgar_filings",str(CIK),"10-K_Sections"])
        
            Status.CIK = CIK
   
            file_n = os.listdir(datapath)[index]
            Status.file_name = file_n

            x['CIK_File'] = file_n.split("-")[0]
            x['etc'] = file_n.split("-")[2]
            x['CIK_Search'] = CIK
            file_name = windowsOrMac.join([datapath,file_n])
            
            
            raw_10k = json.load(open(file_name, mode = 'r', encoding = 'cp1252'))
            
            # Find Fiscal Year of Report: The downloaded date is not always the same
            regex  =  re.compile(r'(CONFORMED PERIOD OF REPORT)')                  
            matches  =  regex.finditer(raw_10k)
            f = [(x.end()) for x in matches]
            x['LastDate'] = re.sub("[^0-9]", "", raw_10k[f[0]:(f[0]+30)])
            
            Status.Q_LastDate = x['LastDate']
            Status.Q_CIK = x['CIK_Search']
            
            
            x['Year'] = int( x['LastDate'][0:4])
            output_file_name = windowsOrMac.join([intermediatepath,str(CIK)+"_"+str(x['Year'])+"_"+str(x['LastDate'])+"_10Kparts.JSON"])
            print(x['Year']>2005,x['Year'])
            
            if x['Year']>2005:
                try:
                    if not os.path.exists(intermediatepath):
                        os.makedirs(intermediatepath)
    
                    doc_start_pattern  =  re.compile(r'<DOCUMENT>')
                    doc_end_pattern  =  re.compile(r'</DOCUMENT>')
                    type_pattern  =  re.compile(r'<TYPE>[^\n]+')
                    doc_start_is  =  [x.end() for x in doc_start_pattern.finditer(raw_10k)]
                    doc_end_is  =  [x.start() for x in doc_end_pattern.finditer(raw_10k)]
                    
                    doc_types  =  [x[len('<TYPE>'):] for x in type_pattern.findall(raw_10k)]
                    
                    document  =  {}
                    
                    
                    ###############################################################################
                    # 1a) Parse Data for new sections (post 2004)
                    ###############################################################################
                    # Create a loop to go through each section type and save only the 10-K section in the dictionary
                    '''for doc_type, doc_start, doc_end in zip(doc_types, doc_start_is, doc_end_is):
                        if doc_type  =  =  '10-K':
                            document[doc_type]  =  raw_10k[doc_start:doc_end]
                    '''    
                    # keep first 10-K
                    doc_type  =  '10-K'
                    indexxer = doc_type.index(doc_type)
                     
                    document[doc_type]  =  raw_10k[doc_start_is[indexxer]:doc_end_is[indexxer]]
                    del raw_10k
         
                            
                    document['10-K'] = re.sub('<[^>]+>', ' ', document['10-K'])        
                    document['10-K'] = re.sub(r"Item\n","Item",document['10-K'])       
                           
                    document['10-K'] = BeautifulSoup(document['10-K'],'lxml').text
                    
                    # Remove common types
                    document['10-K'] = re.sub(r"Item\n"," ",document['10-K'])       
                    document['10-K'] = re.sub(r"((\xa0)|\n|\t|\x96|\x97|\x91|\x92|\x93|\x94|\x95|\x86)"," ",document['10-K'])       
                    document['10-K'] = re.sub(r'"',"", document['10-K'])        
                    document['10-K'] = re.sub(r'“',"", document['10-K'])        
                    document['10-K'] = re.sub(r'(‑|-|—|–)',"", document['10-K'])        
                    document['10-K'] = re.sub(r'also',"", document['10-K']) # Remove stop word
                    document['10-K'] = re.sub(r'ByITEM',"Item", document['10-K'], flags = re.I)
                    document['10-K'] = re.sub(r'ril 19,'," ", document['10-K'], flags = re.I) #Weird inconsistent notation for 1283140a
            
                    document['10-K'] = re.sub(r"Part( |\s|\s+)(1|I|2|II)(\.|\,|)","",document['10-K'], flags = re.I)

                   
                    #document['10-K']=re.sub(r"\n"," ",document['10-K'])       
                    document['10-K']=re.sub(r"&nbsp;"," ",document['10-K'])       
                    #document['10-K']=re.sub(r"(\xa0)","  ",document['10-K'])
                    document['10-K']=re.sub(r"(ITEM)"," ITEM ",document['10-K'], flags=re.I)
                    document['10-K']=re.sub(r"Page","",document['10-K'], flags=re.I)
                    document['10-K']=re.sub(r"(with the SEC.)","",document['10-K'], flags=re.I)
                    document['10-K']=re.sub(r"FORWARDLOOKING COMMENTS              item 1. Business","",document['10-K'], flags=re.I)
                    document['10-K']=re.sub(r"(above)","",document['10-K'], flags=re.I)
                    document['10-K']=re.sub(r" (See .|Se. |See. |S ee|See 40|See I.) "," See ",document['10-K'], flags=re.I)
                    document['10-K']=re.sub(r"SIGNATURES(\s|\s+)Part"," ",document['10-K'], flags=re.I)
                    document['10-K']=re.sub("Annual Report to Unitholders  Part II","",document['10-K'], flags=re.I) # Some 10Ks refernce items and create problems for algorithm.
                    document['10-K']=re.sub(r"Item1. Business        General","Item 1. Business",document['10-K'], flags=re.I)
                    document['10-K']=re.sub(r"Form( |\s|\s+)10K\,","",document['10-K'], flags=re.I)
                   
                    document['10-K']=re.sub(r"(Item)"," Item ",document['10-K'], flags=re.I)
                    document['10-K']=re.sub(r"(in \.|i\.)","in ",document['10-K']) # Remove random ., 
                    
                    document['10-K']=re.sub(r"((review carefully)|(in the section)|(reference?!(.)|(described in section)|(in our)|(in (i))|by|review(ed)|caption(|s|ed)|located|at|particularly|refer|entitled|read|preceding|as|in|to|unde|under|and|of|with|the|including)(\s|\s+)Item)|(in(\s|\s+)this(\s|\s+)item)","",document['10-K'], flags=re.I) # Some 10Ks refernce items and create problems for algorithm.
                    document['10-K']=re.sub(r"((\s)See(\s|s+)item)|(See  Item)|(See   Item)"," ",document['10-K'], flags=re.I) # Some 10Ks refernce items and create problems for algorithm.
                    document['10-K']=re.sub(r"(See(\s|s+)item)|(See  Item)|(See   Item)","",document['10-K'], flags=re.I) # Some 10Ks refernce items and create problems for algorithm.
                    document['10-K']=re.sub(r"See above item","",document['10-K'], flags=re.I) # Some 10Ks refernce items and create problems for algorithm.
                    document['10-K']=re.sub(r"(item)"," item ",document['10-K'],flags=re.I) 
                    document['10-K']=re.sub(r"\,(\s|\s+)Item","",document['10-K'],flags=re.I) 
                    document['10-K']=re.sub(r"(INDEX|CONTENT(S))( |\s|\s+)PART","",document['10-K'],flags=re.I) 
                    document['10-K']=re.sub(r"([0-9]|INDEX|CONTENT(S))( |\s|\s+)PART","",document['10-K'],flags=re.I) 
                    document['10-K']=re.sub(r"FORWARDLOOKING COMMENTS              item 1. Business","",document['10-K'], flags=re.I)                    
                     
                    
                    document['10-K']=re.sub(r"Index to Financial Statements( |\s|\s+)Part (1|I)","",document['10-K'], flags=re.I)                    
                    
                    document['10-K']=re.sub(r"Table of Contents( |\s|\s+)Part I","",document['10-K'], flags=re.I)
                    
                    document['10-K']=re.sub(r"ITEM( |\s|\s+)1A.( |\s|\s+)RISK( |\s|\s+)FACTORS( |\s|\s+)(|\()Continued(|\))","",document['10-K'], flags=re.I)
                    document['10-K']=re.sub(r"Item( |\s|\s+)1.( |\s|\s+)BUSINESS( |\s|\s+)(|\()Continued(|\))","",document['10-K'], flags=re.I)
                    document['10-K']=re.sub(r"ITEM( |\s|\s+)2.( |\s|\s+)Properties( |\s|\s+)(|\()Continued(|\))","",document['10-K'], flags=re.I)



                    document['10-K']=re.sub(r" I.(\s|\s+)Item","",document['10-K'])
                    document['10-K']=re.sub("See(|.)(\s|\s+)(\s|\s+)Item","",document['10-K'], flags=re.I) # Some 10Ks refernce items and create problems for algorithm.
                    document['10-K']=re.sub(",”(\s|\s+)Item","",document['10-K'], flags=re.I) # Some 10Ks refernce items and create problems for algorithm.
                    document['10-K']=re.sub(" index(\s|\s+)Part","",document['10-K'], flags=re.I) # Some 10Ks refernce items and create problems for algorithm.
                    document['10-K']=re.sub(" index(\s|\s+)Part","",document['10-K'], flags=re.I) # Some 10Ks refernce items and create problems for algorithm.
                    document['10-K']=re.sub("Sulfur dioxide.(\s+)v(\s+)PART I","",document['10-K'], flags=re.I) # Some 10Ks refernce items and create problems for algorithm.
                    
                    
                    document['10-K']=re.sub("enterprise.(\s+)Our business","enterprise.",document['10-K'], flags=re.I) # Some 10Ks refernce items and create problems for algorithm.
                    
                    
                                        

                    document['10-K']=re.sub(" the(\s|s\+)heading(|s)(\s|\s+)item","",document['10-K'], flags=re.I) # Some 10Ks refernce items and create problems for algorithm.
                    document['10-K']=re.sub(r"item 1, Business, for","",document['10-K']) # cheap fix   2010, '97472'], 
                    document['10-K']=re.sub(r"\s+"," ",document['10-K']) # cheap fix   2010, '97472'], 
                    
                    document['10-K']=re.sub(r"in \. item"," ",document['10-K']) # cheap fix   2010, '97472'], 
                    document['10-K']=re.sub(r"See 40 item 1"," ",document['10-K']) # cheap fix   2010, '97472'], 
                    document['10-K']=re.sub(r"((with our)|(titled)|(Overview)|\(Continued\)|describe(d)|review(|ed))(\s|s+)item"," ",document['10-K'], flags=re.I) # Some 10Ks refernce items and create problems for algorithm.
                    document['10-K']=re.sub(r"DESCRIPTION OF BUSINESS","Business",document['10-K'], flags=re.I) # Replace some companies alternative
                   
                    document['10-K']=re.sub(r"(Pro perties)|(P ROPERTIES)","Properties",document['10-K'], flags=re.I) # Replace frequent typo.                    
                    document['10-K']=re.sub(r"I t em|It e m|I te m|I tem|It em|Ite m","Item",document['10-K'], flags=re.I) # Replace frequent typo.
                    document['10-K']=re.sub(r"R isk|Ri sk|Ris k","Risk",document['10-K'], flags=re.I) # Replace frequent typo.
                    document['10-K']=re.sub(r"B U S INESS|BU SI NESS|Bu sines s|B usiness|Bu siness","Business",document['10-K'], flags=re.I) # Replace frequent typo.
                    document['10-K']=re.sub(properties_1,"Properties",document['10-K'], flags=re.I) # Replace frequent typo.                    
                    document['10-K']=re.sub(item_1,"item",document['10-K'], flags=re.I) # Replace frequent typo.                    
                    document['10-K']=re.sub(risk_1,"risk",document['10-K'], flags=re.I) # Replace frequent typo.                    
                    document['10-K']=re.sub(business_1,"business",document['10-K'], flags=re.I) # Replace frequent typo.                    
                  
                    
                    # drop ending this report has been signed below by the following 
                    document['10-K']=re.sub(r"( Factors ; item 7,|item 1A, Risk Factors , of this)"," ",document['10-K'], flags=re.I) # Replace frequent typo.
                    document['10-K']=re.sub(r"(No preferred stock was issued or outstanding.( |\s|\s+)Our Business)","No preferred stock was issued or outstanding",document['10-K'], flags=re.I) # Replace frequent typo.
                    


                    document['10-K']=re.sub(r"(Table of Contents BUSINESS Universal American)|(Our Business We are (a|one) ?!(provider of investment management))","Item 1. Business",document['10-K'], flags=re.I) # 1514128 CIK problem

                    
                    document['10-K']=re.sub(r"Overview of Our Business|Introduction and Corporate Overview","Business",document['10-K'], flags=re.I) #CJ eNERGY SOLUTIONS
                    document['10-K']=re.sub(r"General Corporate Structure and Business and Other Information","Business",document['10-K'], flags=re.I) #CJ eNERGY SOLUTIONS
                    document['10-K']=re.sub(r"itemS 1 & 2. BUSINESS and PROPERTIES BUSINESS","Item 1. Business",document['10-K'], flags=re.I) #CJ eNERGY SOLUTIONS
                    document['10-K']=re.sub(r"item.1. BUSINESS","Item 1. Business",document['10-K'], flags=re.I) #CJ eNERGY SOLUTIONS
                    document['10-K']=re.sub(r"PART I item 1. B usin ess","Item 1. Business",document['10-K'], flags=re.I) #CJ eNERGY SOLUTIONS
                    document['10-K']=re.sub(r"(item l. Business)|(item (l|I)      BUSINESS)|(item l Business)|(Sulfur dioxide. v 1. Business)","Item 1. Business",document['10-K'], flags=re.I) #CJ eNERGY SOLUTIONS
                    document['10-K']=re.sub(r"item I","item 1",document['10-K'], flags=re.I) 

                    document['10-K']=re.sub(r"item1.A","Item 1A",document['10-K'], flags=re.I) #CJ eNERGY SOLUTIONS
                    document['10-K']=re.sub(r"item1.B","Item 1B",document['10-K'], flags=re.I) #CJ eNERGY SOLUTIONS
                    document['10-K']=re.sub(r"TRANSATLANTIC HOLDINGS, INC. AND SUBSIDIARIES","",document['10-K'], flags=re.I) #CJ eNERGY SOLUTIONS
                    document['10-K']=re.sub(r"affairs. 1. Business Organization Behringer","affairs. Item 1. Business Organization Behringer",document['10-K'], flags=re.I) #CJ eNERGY SOLUTIONS
                    
                    document['10-K']=re.sub(r"Overview Item 1.(| |\s|\s+)Business","",document['10-K'], flags=re.I) #CJ eNERGY SOLUTIONS
                    document['10-K']=re.sub(r"item 1. General Cypress","item 1. Business General Cypress",document['10-K'], flags=re.I) #CJ eNERGY SOLUTIONS
                    document['10-K']=re.sub(r"Item 1. Business wholesale generator with","Our Business We are a wholesale generator with",document['10-K'], flags=re.I) #CJ eNERGY SOLUTIONS
                    document['10-K']=re.sub(r"item I. BUSINESS","item 1. BUSINESS",document['10-K'], flags=re.I) #CJ eNERGY SOLUTIONS
                    document['10-K']=re.sub(r"item. 1","item 1.",document['10-K'], flags=re.I) #CJ eNERGY SOLUTIONS
                    document['10-K']=re.sub(r" item 1\.+\."," item 1.",document['10-K'], flags=re.I) #CJ eNERGY SOLUTIONS
                    document['10-K']=re.sub(r"(Item(|s) (1|I)(|\s|\s+)(|.) and 2(|\s|\s+)(|.) Business and Properties)"," item 1. business ",document['10-K'], flags=re.I) #CJ eNERGY SOLUTIONS
                    document['10-K']=re.sub(r"1A.risk","1A. Risk",document['10-K'], flags=re.I) #CJ eNERGY SOLUTIONS
                    document['10-K']=re.sub(r"item 1. OUR business","item 1. business",document['10-K'], flags=re.I) #CJ eNERGY SOLUTIONS
                    document['10-K']=re.sub(r"item 1 /","item 1.",document['10-K'], flags=re.I) 
                    document['10-K']=re.sub(r"item 2 /","item 2.",document['10-K'], flags=re.I) 
                    document['10-K']=re.sub(r"item 3 /","item 3.",document['10-K'], flags=re.I) 
                    document['10-K']=re.sub(r"item 4 /","item 4.",document['10-K'], flags=re.I) 
                    document['10-K']=re.sub(r"iteml.","item 1.",document['10-K'], flags=re.I) 
                    document['10-K']=re.sub(r"section of . Item 1.","section of",document['10-K'], flags=re.I) 
                    document['10-K']=re.sub(r"Item 1Business","Item 1. Business",document['10-K'], flags=re.I) 
                    
                    document['10-K']=re.sub(r"item 1. business  business Operations Other Regulation ","business Operations Other Regulation",document['10-K'], flags=re.I) 
                    document['10-K']=re.sub(r"item 1. \& 2.","item 1.",document['10-K'], flags=re.I) 

                    
                     
                    document['10-K']=re.sub(r"item 1 I business ","Item 1. business",document['10-K'], flags=re.I) 
                    document['10-K']=re.sub(r"item 1A. I risk Factors ","item 1A. risk Factors ",document['10-K'], flags=re.I) 
                    document['10-K']=re.sub(r"item 2. I Properties","Item 2. Properties",document['10-K'], flags=re.I) 
                    document['10-K']=re.sub(r"item 7. I Management","item 7. Management",document['10-K'], flags=re.I) 
                    document['10-K']=re.sub(r"item 7A. I Quantitative","item 7A. Quantitative",document['10-K'], flags=re.I) 
                    document['10-K']=re.sub(r"item S 1 AND 2 . business","Item 1. business",document['10-K'], flags=re.I) 
                    document['10-K']=re.sub(r"item 2","item 2. ",document['10-K'], flags=re.I) 
                    document['10-K']=re.sub(r"item 1A","item 1A. ",document['10-K'], flags=re.I) 
                    document['10-K']=re.sub(r"item 1B","item 1B. ",document['10-K'], flags=re.I) 
                    document['10-K']=re.sub(r"item 7","item 7. ",document['10-K'], flags=re.I) 
                    document['10-K']=re.sub(r"item 7A","item 7A. ",document['10-K'], flags=re.I) 
                    document['10-K']=re.sub(r"(\.)\s(\.)",".",document['10-K'], flags=re.I) 
                    document['10-K']=re.sub(r'under the caption risks Factors,',"",document['10-K'], flags=re.I)  # Oil States
                    document['10-K']=re.sub(r'(item s 1. and 2. business,)|(item s 1. and 2. business)',"item 1. business",document['10-K'], flags=re.I)  # Oil States
                    document['10-K']=re.sub(r'L EGAL P ROCEEDINGS',"Legal Proceedings",document['10-K'], flags=re.I)  # Oil States
                    document['10-K']=re.sub(r'item No.',"item ",document['10-K'], flags=re.I)  # Oil 1022469
                    document['10-K']=re.sub(r'item No.',"item ",document['10-K'], flags=re.I)  # Oil 1022469
                    document['10-K']=re.sub(r'item S',"items",document['10-K'], flags=re.I)  # Oil 1022469
                    document['10-K']=re.sub(r'items 1 \& 2 business',"item 1. business ",document['10-K'], flags=re.I)  # Oil 1022469
                    document['10-K']=re.sub(r'Overview We are a leading provider of advanced',"item 1. business Overview We are a leading provider of advanced",document['10-K'], flags=re.I)  # For CIK 1580808
                    document['10-K']=re.sub(r'Business Overview We are a leading provider of software and hardware solutions designed',"item 1. business Overview We are a leading provider of software and hardware solutions designed",document['10-K'], flags=re.I)  # For CIK 1580808
                    document['10-K']=re.sub(r'item 1.business','item 1. business',document['10-K'], flags=re.I)  # For CIK 1580808
                    document['10-K']=re.sub(r'items 1., 1A., and 2. business','item 1. business',document['10-K'], flags=re.I)  # For CIK 1580808
                    document['10-K']=re.sub(r'items 1. business overview','item 1. business overview',document['10-K'], flags=re.I)  # For CIK 1580808
                    document['10-K']=re.sub(r'item 1A.  . risk ','item 1A. Risk',document['10-K'], flags=re.I)  # For CIK 1580808
                    document['10-K']=re.sub(r'item 1 . business','item 1. business',document['10-K'], flags=re.I)  # For CIK 1580808
                    document['10-K']=re.sub(r'item 2.  . ','item 2. ',document['10-K'], flags=re.I)  # For CIK 1580808
                    document['10-K']=re.sub(r'item 7. A. ','item 7A. ',document['10-K'], flags=re.I)  # For CIK 1580808
                    document['10-K']=re.sub(r'item 8 . ','Item 8.',document['10-K'], flags=re.I)  # For CIK 1580808
                    document['10-K']=re.sub(r'item . 1','item 1.',document['10-K'], flags=re.I)  # For CIK 1580808
                    document['10-K']=re.sub(r'items 1. and 2 . business ','item 1. business',document['10-K'], flags=re.I)  # For CIK 1580808
                    document['10-K']=re.sub(r'1. business Our Company Oil States','item 1. business Our Company Oil States',document['10-K'], flags=re.I)  # For CIK 1580808
                    document['10-K']=re.sub(r'items 1 & 2. business','item 1. business',document['10-K'], flags=re.I)  # For CIK 1580808
                    document['10-K']=re.sub(r'items 1 And 2 business and Properties','item 1. business and Properties',document['10-K'], flags=re.I)  # For CIK 1580808
                    document['10-K']=re.sub(r'item .1. business','item 1. business',document['10-K'], flags=re.I)  # For CIK 1580808
                    document['10-K']=re.sub(r' further in I. item 1A',' further in  1A',document['10-K'], flags=re.I)  # For CIK 1580808
                    document['10-K']=re.sub(r' item 1A. risk Factors of this Annual','1A. risk Factors of this Annual',document['10-K'], flags=re.I)  # For CIK 1580808

                    document['10-K']=re.sub(r'business ; item 1A','business; 1A',document['10-K'], flags=re.I)  # For CIK 1580808
                    document['10-K']=re.sub(r'”; item 1A.','”; 1A.',document['10-K'], flags=re.I)  # For CIK 1580808
                    document['10-K']=re.sub(r'and ‘‘ item 1A','and ‘‘ 1A',document['10-K'], flags=re.I)  # For CIK 1580808
                    document['10-K']=re.sub(r'See ‘‘ Item 1','See ‘‘ 1',document['10-K'], flags=re.I)  # For CIK 1580808
  
                    document['10-K']=re.sub(r'under ‘‘ item','under ',document['10-K'], flags=re.I)  # For CIK 1580808

                    document['10-K']=re.sub(r'Other Information item 1A.','Other Information ',document['10-K'], flags=re.I)  # For CIK 1580808
                    document['10-K']=re.sub(r'read I item 1A','read I',document['10-K'], flags=re.I)  # For CIK 1580808

                    document['10-K']=re.sub(r'Framework, ” item 1A. risk Factors','Framework, ” risk Factors',document['10-K'], flags=re.I)  # For CIK 1580808
                    document['10-K']=re.sub(r'and I. item 1A.','and ',document['10-K'], flags=re.I)  # For CIK 1580808
                    document['10-K']=re.sub(r'S ee item','See',document['10-K'], flags=re.I)  # For CIK 1580808
                    document['10-K']=re.sub(r'in \(i\) item 1A.','in',document['10-K'], flags=re.I)  # For CIK 1580808
                    document['10-K']=re.sub(r'this report: item 1A.','this report:',document['10-K'], flags=re.I)  # For CIK 1580808
                    document['10-K']=re.sub(r'in this including I item','in this including',document['10-K'], flags=re.I)  # For CIK 1580808
                  
                    document['10-K']=re.sub(r'and item 1A.','and',document['10-K'], flags=re.I)  # For CIK 1580808
                    
                    document['10-K']=re.sub(r'described in 42 item 1A.','described in',document['10-K'], flags=re.I)  # For CIK 1580808
                    document['10-K']=re.sub(r'See \'\' item 1A.','See',document['10-K'], flags=re.I)  # For CIK 1580808
                    document['10-K']=re.sub(r'Table of Contents heading item 1A. risk Factors','heading',document['10-K'], flags=re.I)  # For CIK 1580808
                    document['10-K']=re.sub(r'aforementioned item 1A. risk Factors','aforementioned',document['10-K'], flags=re.I)  # For CIK 1580808
                    document['10-K']=re.sub(r'; item 1A. risk Factors; 7. Man','Man',document['10-K'], flags=re.I)  # For CIK 1580808
                    document['10-K']=re.sub(r'but not limited to: item 1A. risk Factors','but not limited to:',document['10-K'], flags=re.I)  # For CIK 1580808

                
                    document['10-K']=re.sub(r'item 1. DESCRIPTION OF business','Item 1. Business',document['10-K'], flags=re.I)  # For CIK 1580808

                    
                    if int(CIK)==70530:
                        document['10-K']=re.sub(r'in of Form 10K item 1A','in of',document['10-K'], flags=re.I)  # For CIK 1580808

                    if int(CIK)==77776:
                        document['10-K']=re.sub(r'See 57 Table of Contents item 1A','See 57 Table of Contents',document['10-K'], flags=re.I)  # For CIK 1580808
                   
                    if int(CIK)==811156:
                        document['10-K']=re.sub(r'Information; item 1A.','Information;',document['10-K'], flags=re.I)  # For CIK 1580808
                    if int(CIK)==709283:
                        document['10-K']=re.sub(r'Cautionary Statements item','Cautionary Statements',document['10-K'], flags=re.I)  # For CIK 1580808
                     
                    
                    if int(CIK)==1616817:
                        document['10-K']=re.sub(r'transactions. item','transactions.',document['10-K'], flags=re.I)  # For CIK 1580808
                    
                    
                    if int(CIK)==1603923:
                        document['10-K']=re.sub(r'[0-9] Table of Contents item 1A. RiskFactors',' ',document['10-K'], flags=re.I)  # For CIK 1580808
                        document['10-K']=re.sub(r'• item 1A.',' ',document['10-K'], flags=re.I)  # For CIK 1580808

                    if int(CIK)==1724965:
                       document['10-K']=re.sub(r'items 1 and 2. bus','item 1. bus',document['10-K'], flags=re.I)  # For CIK 1580808
                       document['10-K']=re.sub(r'Properties; item 1A. ','Properties; ',document['10-K'], flags=re.I)  # For CIK 1580808


                    if int(CIK)==1320461:
                        document['10-K']=re.sub(r'General: The Company','item 1. business General: The Company',document['10-K'], flags=re.I)  # For CIK 1580808
                    if int(CIK)==1443646 and x['Year']>2018:
                        document['10-K']=re.sub(r'overview For','item 1. business overview For',document['10-K'], flags=re.I)  # For CIK 1580808

                    if int(CIK)==1489137 and x['Year']>2011:
                        document['10-K']=re.sub(r'Our business We are ','item 1. business Our business We are ',document['10-K'], flags=re.I)  # For CIK 1580808
                    if int(CIK)==1474464 and x['Year']>2011:
                        document['10-K']=re.sub(r'Organization We','item 1. business Organization We',document['10-K'], flags=re.I)  # For CIK 1580808

                    if int(CIK)==1467027:
                        document['10-K']=re.sub(r'business The Company','item 1. business The Company',document['10-K'], flags=re.I)  # For CIK 1580808

                    if int(CIK)==1379661:
                        document['10-K']=re.sub(r'(1. business. Targa Resources)|(Tex 1. business)','item 1. business',document['10-K'], flags=re.I)  # For CIK 1580808
 
                    if int(CIK)==1397821:
                        document['10-K']=re.sub(r'business. Overview Duff','item 1. business business. Overview Duff',document['10-K'], flags=re.I)  # For CIK 1580808

 
                    if int(CIK)==1297587 and x['Year']==2016:
                        document['10-K']=re.sub(r'ABOUT US Gramercy Property ','item 1. business ABOUT US Gramercy Property ',document['10-K'], flags=re.I)  # For CIK 1580808
                    if int(CIK)==1545654:
                        document['10-K']=re.sub(r'items 1 & 2. business AND Properties Overview','item 1. business AND Properties Overview',document['10-K'], flags=re.I)  # For CIK 1580808

                    if int(CIK)==1538849:
                        document['10-K']=re.sub(r'items 1., 1A. and 2. business , risk FACTORS AND Properties Overview ','item 1. business , risk FACTORS AND Properties Overview ',document['10-K'], flags=re.I)  # For CIK 1580808
                        document['10-K']=re.sub(r'Risk Factors If','item 1A. Risk Factors If',document['10-K'], flags=re.I)  # For CIK 1580808


                    if int(CIK)==1562039:
                        document['10-K']=re.sub(r'Risk Factors If','item 1A. Risk Factors If',document['10-K'], flags=re.I)  # For CIK 1580808
                    if int(CIK)==1583103:
                        document['10-K']=re.sub(r'items 1 & 2. business','item 1. business',document['10-K'], flags=re.I)  # For CIK 1580808



                    if (CIK)!=int(1140859) and (CIK)!=int(203248) and (CIK)!=int(316253) and  (CIK)!=int(1419178) and (CIK)!=int(1401257) and int(CIK)!=int(1379378) and int(CIK)!=int(1273685) and int(CIK)!=int(1265888) and int(CIK)!=int(1212545) and int(CIK)!=int(1616318) and int(CIK)!=int(898174) and CIK!=int(4962) and CIK!=int(1710583) and CIK!=int(1705696) and int(CIK)!=int(354190) and int(CIK)!=int(717423):
                        # Note that 1705696 has different formatting so section comes later
                        try:
                            regex=re.compile("this report (has been signed|to be signed) (on its behalf by the undersigned|on behalf of the registrant|below by the following)",re.IGNORECASE)
                        
                            matches = regex.finditer(document['10-K'])
                            last_match = int(pd.DataFrame([(x.group(), x.start(), x.end()) for x in matches])[1].iloc[-1])
                            document['10-K']=document['10-K'][0:last_match]
                        except:
                            print('not remove sections after signature')
                        # Remove cross-index
                        try:
                            regex=re.compile("CROSS REFERENCE INDEX This Annual Report and",re.IGNORECASE)
                        
                            matches = regex.finditer(document['10-K'])
                            last_match = int(pd.DataFrame([(x.group(), x.start(), x.end()) for x in matches])[1].iloc[-1])
                            document['10-K']=document['10-K'][0:last_match]
                        except:
                            print('not remove sections after signature')
                            
                           
                            
                        try:
                            regex=re.compile("(In this combined report and from time to time)|(Table of Contents CAUTIONARY NOTE REGARDING FORWARDLOOKING STATEMENTS)|(This report on Form 10K contains certain|The use of any statements containing the words anticipate|This report on Form 10 K contains certain forward looking statements|This report on Form 10K, including information incorporated herein by reference|contains forward(|-)looking statements as defined in the)",re.IGNORECASE)
                            matches = regex.finditer(document['10-K'])
                            first_match = int(pd.DataFrame([(x.group(), x.start(), x.end()) for x in matches])[1].iloc[0])
                            document['10-K']=document['10-K'][first_match:]
                        except:
                            print('not remove before forward looking')
                       
                    if int(CIK)==1618756 and x['Year']<2016:
                        document['10-K']=re.sub(r'business Company Overview ',"Item 1. business Company Overview ",document['10-K'], flags=re.I)  # Oil States
                    if int(CIK)==18349:
                        document['10-K']=re.sub(r'practices. item 1A. risk Factors',"practices. risk Factors",document['10-K'], flags=re.I)  # Oil States
                    if int(CIK)==85961:
                        document['10-K']=re.sub(r'item 1A. Risk FACTORS The risks included in',"The risks included in",document['10-K'], flags=re.I)  # Oil States
                        
           
                       
                    
                    item1='((Item|ITEM)(|\s|\s+)(1|I)(|\s|\s+)(|.|,)(\s|\s+)(Bus))|(items (1|I)(|.) and 2. Business and Properties)|(Item 1. Bus)'
                    item1A='(Item|ITEM)(|\s|\s+)(1A|1\(a\))(| )(.|,|)(\s|\s+)(Ri)'
                    item1B='(Item|ITEM)(|\s|\s+)(1)(|B)(|\s|\s+)(.|,|)(\s|\s+)(UN)'
                    item2='(Item|ITEM)(|\s|\s+)(2)(|\s|\s+)(|.|,)(\s|\s+)(Prop|Fac)'
                    item3='(Item|ITEM)(\s|\s+)(3)(.|,)(\s|\s+)(Le)'
                    item4='(Item|ITEM)(\s|\s+)(4)(.|,)(\s|\s+)(Mi)'
                    item5='(Item|ITEM)(\s|\s+)(5)(.|,)(\s|\s+)(Mar)'
                    item6='(Item|ITEM)(\s|\s+)(6)(.|,)(\s|\s+)(Se)'
                    item7='(Item|ITEM)(\s|\s+)(7)(.|,)(\s|\s+)'
                    item7A='(Item|ITEM)(\s|\s+)(7A)(.|,)(\s|\s+)(Qu)'
                    item7B='(Item|ITEM)(\s|\s+)(7B)(.|,)(\s|\s+)(Fi)'
                    item8='(Item|ITEM)(\s|\s+)(8)(.|,)(\s|\s+)(Fi)'
                    item9='(Item|ITEM)(\s|\s+)(9)(.|,)'
                    item9A='(Item|ITEM)(\s|\s+)(9A)(.|,)(\s|\s+)(Co)'
                    item9B='(Item|ITEM)(\s|\s+)(9B)(.|,)(\s|\s+)(Ot)'
                    item10='(Item|ITEM)(\s|\s+)(10)(.|,)(\s|\s+)(Di)'
        
                    
                    comb="|".join([item1,item1A,item1B,item2,item3,item4,item5,item6,item7,item7A,item7B,item8,item9,item9A,item9B,item10])
                    # Fix for Morgan Stanley: MS is the only major firm (yet) that does not include item 1, instead just titles.
                    # I manually verify
                    
                    if int(CIK)==1026214 and x['Year']>2014:
                        
                        item1='ABOUT FREDDIE MAC Fredd'
                        item1A='risk Factors The'
                        item2='Legal Proceedings We'
                            
                        comb="|".join([item1,item1A,item2])

                    if int(CIK)==831001 and x['Year']>2013:
                        
                        item1='(?<!risk) OVERVIEW( |\s|\s+)Citigroup'
                        item1A='RISK FACTORS( |\s|\s+)The following '
                        item2='(Managing Global Risk Table of Contents (?!([0-9])|Credit))'

                        comb="|".join([item1,item1A,item1B,item2])

                    if int(CIK)==895421 and x['Year']>2016:
                        
                        item1='Entergy is an integrated energy company'
                        item1A='Risk Factors For a disc'
                        item1B='Table of Contents Selected Financial Data '
                        item2='We make markets in various commodity'

                        comb="|".join([item1,item1A,item1B,item2])
                    
                    
                    if int(CIK)==895421 and x['Year']>2016:
                        
                        item1='Entergy is an integrated energy company'
                        item1A='Risk Factors For a disc'
                        item1B='Table of Contents Selected Financial Data '
                        item2='We make markets in various commodity'

                        comb="|".join([item1,item1A,item1B,item2])
                    
                    
                    if int(CIK)==1313024:
                        
                        item1='item 1. 2. Business and Properties We'
                        item1A='item 1A. Risk'
                        item2='(Item|ITEM)(\s|\s+)(3)(.|,)(\s|\s+)(Le)'

                        comb="|".join([item1,item1A,item2])
                    # Same Issue with Cardinal health
                    if int(CIK)==895421 and x['Year']>2016:
                        
                        item1='Table of Contents Business Overview'
                        item1A='Risk Factors For a disc'
                        item1B='Table of Contents Selected Financial Data '
                        item2='We make markets in various commodity'

                        comb="|".join([item1,item1A,item1B,item2])
                    # Same Issue with Cardinal health
                    if int(CIK)==721371 and x['Year']>2014:
                        
                        item1='Business Business General'
                        item1A='Risk Factors Risk Factors The'
                        item2='Properties and Legal Proceedings Properties '

                        comb="|".join([item1,item1A,item2])

                    if int(CIK)==1110805 and x['Year']>2005:
                        
                        item1='(BUSINESS, RISK FACTORS(|,) AND PROPERTIES OVERVIEW)|(Item 1. Bus)'
                        item1A='(RISK FACTORS RISKS RELATED TO OUR BUSINESS)|(RISK FACTORS RISKS RELATED TO THE POTENTIAL MERGER)'
                        item2='PROPERTIES Our principal'

                        comb="|".join([item1,item1A,item2])
       
                    if int(CIK)==1035002 and x['Year']<2014:
                        
                        item1='BUSINESS, RISK FACTORS(,|) AND PROPERTIES Overview'
                        item1A='Table of Contents RISK FACTORS'
                        item2='PROPERTIES Our'

                        comb="|".join([item1,item1A,item2])
                    if int(CIK)==65100 and x['Year']<2008:
                        
                        item1='Overview Merrill|Introduction Merrill'                    
                        item1A='Risk Factors that Could Affect Our Business In'
                        item2='Properties Merrill|Properties We have'

                        comb="|".join([item1,item1A,item2])


                    if int(CIK)==40545 and x['Year']>2012:
                        
                        item1='ABOUT GENERAL ELECTRIC (OUR Business|General Electric|We are a leading global)'                    
                        item1A='RISK FACTORS The following'
                        item2='LEGAL PROCEEDINGS (Refer|(In|I n) addition)|LEGAL PROCEEDINGS Refer to|LEGAL PROCEEDINGS( |\s\|\s+)WMC| LEGAL PROCEEDINGS There are'

                        comb="|".join([item1,item1A,item2])
        
                    if int(CIK)==1469510 and x['Year']<2011:
                        
                        item1='item 1.( |\s|\s+)business( |\s|\s+)RESOLUTE'
                        item1A='Table of Contents item 1A. RISK FACTORS '
                        item2='(stock|warrants). item 3'

                        comb="|".join([item1,item1A,item2])
                    if int(CIK)==65984:
                        #
                        item1="(BUSINESS Entergy is an integrated energy company)|(164 ENTERGY'S BUSINESS \(continued from 3\) )"
                        item1A='(RISK FACTORS Investors)|(FACTORS Entergy and its subsidiaries operate in a market environment that involves significant risks)'
                        item2="(([0-9][0-9][0-9]) ENTERGY ARKANSAS, INC. MANAGEMENT'S FINANCIAL DISCUSSION AND ANALYSIS Results of Operations)|(Table of Contents ENTERGY ARKANSAS, INC. AND SUBSIDIARIES MANAGEMENT’S FINANCIAL DISCUSSION AND ANALYSIS Plan)|(ENTERGY ARKANSAS, LLC AND SUBSIDIARIES MANAGEMENT’S FINANCIAL DISCUSSION AND ANALYSIS Results of Operations )|(ENTERGY ARKANSAS, INC. MANAGEMENT'S FINANCIAL DISCUSSION AND ANALYSIS Results of Operations)|(ENTERGY ARKANSAS, LLC AND SUBSIDIARIES MANAGEMENT’S FINANCIAL DISCUSSION AND ANALYSIS Internal Restructuring )|(ENTERGY ARKANSAS, INC. AND SUBSIDIARIES MANAGEMENT’S FINANCIAL DISCUSSION AND ANALYSIS Results of Operations Net Income)|(ENTERGY ARKANSAS, INC. MANAGEMENT'S FINANCIAL DISCUSSION AND ANALYSIS Results of Operations Net Income)"
                        #\s(ENTERGY ARKANSAS, INC. MANAGEMENTS FINANCIAL DISCUSSION AND ANALYSIS Results of Operations)"
                        comb="|".join([item1,item1A,item2])
        
                    if int(CIK)==827052 and x['Year']>2013:
                        #
                        item1="BUSINESS CORPORATE STRUCTURE, INDUSTRY AND OTHER INFORMATION "
                        item1A='RISK FACTORS RISKS RELATING TO EDISON INTERNATIONAL Edison '
                        item2="PROPERTIES As a holding company"

                        comb="|".join([item1,item1A,item2])
        
                    if int(CIK)==int(893955):
                        
                        item1="(?<!See) OTHER BUSINESS AND INDUSTRY INFORMATION (?![0-9])"
                        item1A='(RISK FACTORS The following)|(Risk Factors( |\s|\s+)Certain)'
                        item2="Corporate Governance and Controls"

                        comb="|".join([item1,item1A,item2])
       
                    if int(CIK)==int(36104) and x['Year']==2006:
                        
                        item1="General Business Description U.S."
                        item1A='Company Risk Factors'
                        item2="CAPITAL COVENANTS The"

                        comb="|".join([item1,item1A,item2])
       
                    if int(CIK)==int(36104) and x['Year']==2006:
                        
                        item1="General Business Description U.S."
                        item1A='Company Risk Factors'
                        item2="CAPITAL COVENANTS The"

                        comb="|".join([item1,item1A,item2])
       
                    if int(CIK)==int(45012) and x['Year']>2006 and x['Year']<2010:
                        
                        item1='(item1. business. General)|(Item 1. Business. General)'
                        item1A='(risk FACTORS Foreign Corrupt)|(RISK FACTORS While it is not)'
                        item2='(item2. Properties. We)|(item 2. Properties. We)'
                        item1b='The management of Halliburton Company'
                        comb="|".join([item1,item1A,item1b,item2])
       
                    if int(CIK)==int(63908) and x['Year']>2018:
                       item1='McDonald’s Corporation, the registrant'
                       item1A='risk FACTORS If we '
                       item2='The Company has pending'
                       item1b='management reviews results on a constant currency'
                       comb="|".join([item1,item1A,item1b,item2])
                    if int(CIK)==int(106535) and x['Year']>2008 :
                       item1='OUR BUSINESS We'
                       item1A='RISK FACTORS( |\s|\s+)We are'
                       item1b='UNRESOLVED STAFF COMMENTS There'
                       comb="|".join([item1,item1A,item1b])
                    if int(CIK)==int(1179929) and x['Year']>2015 :
                       item1='ABOUT MOLINA HEALTHCARE (Molina|Our)'
                       item1A='(You should carefully consider the risks)|(Risks Related to Our Health Plans Segment)'
                       item2='(Properties As of)|(Properties We own)'
                       comb="|".join([item1,item1A,item2])
                    if int(CIK)==1601712 and x['Year']>2017:
                        
                        item1='We are a premier consumer financial services company'
                        item1A='Risks Risk Factors Relating to Our Business'
                        item3='CONSOLIDATED FINANCIAL STATEMENTS AND SUPPLEMENTARY DATA Report '
                        comb="|".join([item1,item1A,item3])
                    if int(CIK)==1223786:
                        
                        item1='(items 1., 1A. and 2. business, risk FACTORS AND Properties OVERVIEW)'
                        item1A='(RISK FACTORS( |\s|\s+)RISKS)'
                        item3='(item 3. legal proceedings We)'
                        item7="(DISCUSSION AND ANALYSIS OF FINANCIAL CONDITION AND RESULTS OF OPERATIONS The)"
                        item7a="(item 7. A. QUANTITATIVE AND QUALITATIVE DISCLOSURES ABOUT MARKET risk (Our|None))"
                        comb="|".join([item1,item1A,item3,item7,item7a])
                    if int(CIK)==1223786:
                        
                        item1='(items 1., 1A. and 2. business, risk FACTORS AND Properties OVERVIEW)'
                        item1A='(RISK FACTORS( |\s|\s+)RISKS)'
                        item3='(item 3. legal proceedings We)'
                        item7="(DISCUSSION AND ANALYSIS OF FINANCIAL CONDITION AND RESULTS OF OPERATIONS The)"
                        item7a="(item 7. A. QUANTITATIVE AND QUALITATIVE DISCLOSURES ABOUT MARKET risk (Our|None))"
                        comb="|".join([item1,item1A,item3,item7,item7a])


                    regex=re.compile(comb,re.IGNORECASE)

                    matches = regex.finditer(document['10-K'])

                    #success

                    test_df = pd.DataFrame([(x.group(), x.start(), x.end()) for x in matches])
                        
                    test_df.columns = ['item', 'start', 'end']
                    test_df['itemkey'] = test_df.item.str.lower()
                    
                    # Display the dataframe
                    test_df.head()
                    #FAIL

                    # Get rid of unnesesary charcters from the dataframe
                    test_df['itemkey'].replace('&#160;',' ',regex=True,inplace=True)
                    test_df['itemkey'].replace('&nbsp;',' ',regex=True,inplace=True)
                    test_df['itemkey'].replace('(\|| |\.|\:|\(|\))','',regex=True,inplace=True)
                    test_df['itemkey'].replace('>','',regex=True,inplace=True)
                    test_df['itemkey'].replace(',','',regex=True,inplace=True)
                    
              
                    
                    if int(CIK)==1223786:
                        test_df.itemkey=['item1bus','item1ari','item3le','item7ma','item7aqu']

                    if (int(CIK)==895421 and x['Year']>2016):
                        
                        test_df.itemkey=['item1bus','item1ari','item1bun','item2prop']
                        
                    if (int(CIK)==1601712 and x['Year']>2017) or (int(CIK)==int(1179929) and x['Year']>2015) or (int(CIK)==int(36104) and x['Year']==2006) or (int(CIK)==1026214 and x['Year']>2014) or (int(CIK)==831001 and x['Year']>2013) or (int(CIK)==int(893955)) or (int(CIK)==827052 and x['Year']>2013) or (int(CIK)==65984) or (int(CIK)==1469510 and x['Year']<2011) or (int(CIK)==1313024) or (int(CIK)==40545 and x['Year']>2012) or (int(CIK)==65100 and x['Year']<2008) or (int(CIK)==721371 and x['Year']>2014) or (int(CIK)==1110805 and x['Year']>2005) or (int(CIK)==1035002 and x['Year']<2014):
                         test_df.itemkey=['item1bus','item1ari','item2prop']

                    if (int(CIK)==int(45012) and x['Year']>2006 and x['Year']<2010) or (int(CIK)==int(63908) and x['Year']>2018):
                        
                        test_df.itemkey=['item1bus','item2prop','item1ari','item1bun']
                    if (int(CIK)==int(106535) and x['Year']>2008):
                        
                        test_df.itemkey=['item1bus','item1ari','item1bun']



                    try:
                        test_df['itemkey']=test_df['itemkey'].replace('item2fac','item2prop')
                    except:
                        print('ok')

                        
                    try:
                        unresolved_keep=test_df[test_df['itemkey']=='item1bun']
                    except:
                        print('no item 1b')
                    try:
                        prop_keep=test_df[test_df['itemkey']=='item2prop']
                    except:
                        print('no item 2')
                   
                    #FAIL

                    test_df['diff']=test_df['start'].diff().shift(-1)
                    test_df=test_df[test_df['diff']>235] # apply
                    
                    test_df=test_df.drop(columns=['diff'])
                    # display the dataframe
                    test_df['itemkey'].head()
                    
                    
                    #FAIL
                    # Drop duplicates
                    pos_dat = test_df.sort_values('start', ascending=True).drop_duplicates(subset=['itemkey'], keep='last')
                    
                    # Occasionally none is used for 1B. Want to kepe these
                    try:
                        closest_item1b=unresolved_keep[ unresolved_keep.start>int(pos_dat[pos_dat.itemkey=="item1ari"].end)]
                        closest_item1b=closest_item1b[closest_item1b.end==closest_item1b.end.min()]      
                        pos_dat=pos_dat.append(closest_item1b)
                    except:
                        print('noitem1b')
                    try:
                        closest_item2=prop_keep[ prop_keep.start>int(pos_dat[pos_dat.itemkey=="item1ari"].end)]
                        closest_item2=closest_item2[closest_item2.end==closest_item2.end.min()]                    
                        pos_dat=pos_dat.append(closest_item2)
                    except:
                        print('noitem2')
                        
                        
                    pos_dat=pos_dat.sort_values(by=['start'])
                    pos_dat=pos_dat.set_index('item')
                    pos_dat['SectionText']=""
                    #Fial
                    try:
                        pos_dat['itemkey']=pos_dat['itemkey'].replace('items1and2businessandproperties','item1')
                    except:
                        pos_dat['itemkey']=pos_dat['itemkey'].replace('item1bus','item1')
                    pos_dat['itemkey']=pos_dat['itemkey'].replace('item1bus','item1')
                    pos_dat=pos_dat.drop_duplicates()
                    pos_dat.drop_duplicates()
                    pos_dat = pos_dat.sort_values('start', ascending=True).drop_duplicates(subset=['itemkey'], keep='last')
                    min_value=int(pos_dat[pos_dat.itemkey=='item1'].start)
                    pos_dat=pos_dat[pos_dat.start>=min_value]      
                    
                # Display the dataframe
                    if int(CIK)==1545654 and x['Year']>2016:
                        pos_dat=pos_dat[pos_dat.itemkey!='item2prop']
 
                
                    itemlist=pos_dat.index
                    itemlist=natsorted(list(itemlist))
                    itemlistkey=pos_dat.itemkey
                    itemlistkey=natsorted(list(itemlistkey))
                
                           
                
                    try:
                        ii=itemlistkey.index("item9a")
                    except:
                        try:
                            ii=itemlistkey.index("item8fi")
                        except:
                            try: 
                                ii=itemlistkey.index("item3le")
                            except:
                                try:
                                    ii=itemlistkey.index("item2prop")
                                except:
                                    ii=itemlistkey.index("item1bun")
                        
                    #FAILS
                    for index2 in range(0,ii):
                        pos_dat.loc[pos_dat.itemkey==itemlistkey[index2],'SectionText'] = document['10-K'][int(pos_dat[pos_dat.itemkey==itemlistkey[index2]]['start']):int(pos_dat[pos_dat.itemkey==itemlistkey[1+index2]]['start'])]
                    pos_dat['itemkey']=pos_dat['itemkey'].replace('item1','item1bus')
    
    
                    pos_dat=pos_dat.set_index('itemkey')
                    pos_dat['TextCleaned']=""
                    pos_dat['TextCleaned']=pos_dat['SectionText'].map(lambda text: BeautifulSoup(text, 'lxml').get_text("\n\n"))
                    
                    
                    x['SectionNew']=pos_dat.to_dict()
                
            ###############################################################################
            # 2) Save file
            ###############################################################################
            
            
                    with open(output_file_name, 'w', encoding ='utf8') as json_file: 
                        json.dump(x, json_file) 
                    Status.Status="Success"
                    Status_Holder=Status_Holder.append(Status)

                except:
                    print("Post 2005 Data exists, but failed")
                    Status.Status="Post2006Failure"
                    Status_Holder=Status_Holder.append(Status)
                    

            else:
                print("Pre-2006 Data")
                Status.Status="Pre2006"
                Status_Holder=Status_Holder.append(Status)
                    
    except:
        print("emptyfolder ", CIK)
        
        emptyfolder.append(CIK)
#raw_10k


    
Status_Holder.to_excel("D:/Debugging/"+re.sub("(\s|:)","",str(datetime.datetime.now()))+"_status_report.xlsx"   )
        
EF=pd.DataFrame(emptyfolder)
name="D:/Debugging/"+re.sub("(\s|:)","",str(datetime.datetime.now()))+"empty_folder.xlsx"
EF.to_excel(name)


###############################################################################
# 3) Load
###############################################################################


# Create holders of all pre2005
pre_holder=[]
unique_CIKS=set(Status_Holder.CIK)
for cik_u in list(unique_CIKS):
    if len(Status_Holder[Status_Holder.CIK==cik_u])==len(Status_Holder[(Status_Holder.CIK==cik_u) & (Status_Holder.Status=="Pre2006")]):
        pre_holder.append(cik_u)




intermediate_folder=windowsOrMac.join([dropboxpath,"intermediate_data",'NYSE'])
clist_aug=list(set(clist)-set(pre_holder))


PH=pd.DataFrame([pre_holder,np.repeat("AllPre2006",len(pre_holder))]).transpose()
EF=pd.DataFrame([emptyfolder,np.repeat("no10KfromEDGAR",len(emptyfolder))]).transpose()
Good=pd.DataFrame([clist_aug,np.repeat("SomePost2005",len(clist_aug))]).transpose()
newf= pd.concat([PH,EF,Good])
newf.columns=["CIK","Status"]
excelname="D:/Debugging/"+re.sub("(\s|:)","",str(datetime.datetime.now()))+"_missing_sections.xlsx"  
newf.to_excel(excelname,sheet_name="Aggregates")

nada=[]
no_example1=[]
no_example1a=[]

no_example_holder = pd.DataFrame(columns = ['CIK','Date','Bus','Risk'])


nofolder=[]
for CIK_t2 in clist_aug:

    c = ['CIK_File','Date','Date','item1bus','item1ari','item1comb']
    df1 = pd.DataFrame(columns=c)
    of=windowsOrMac.join([intermediate_folder,"sec_edgar_filings",str(CIK_t2),"10-K_sections"])
    if os.path.exists(of)==True:
        for index3 in range(0,len(os.listdir(of))):
            try: 
                if int(os.listdir(of)[index3].split('_')[1])>2005:
                    no_example = pd.DataFrame(np.zeros([1,4]),columns = ['CIK','Date','Bus','Risk'])
    
                    X=json.load(open(windowsOrMac.join([of,os.listdir(of)[index3]]), mode='r', encoding='cp1252'))
                    no_example.CIK=CIK_t2
                    no_example.Date=X['LastDate']
                    no_example.Bus="."
                    no_example.Risk="."
                    example1=X['SectionNew']['SectionText']['item1bus']
                    example1a=X['SectionNew']['SectionText']['item1ari']
                    if len( example1 )<100:
                        no_example1.append([ X['LastDate'],CIK_t2])
                        os.listdir(of)
                        no_example.Bus="Missing"
    
                    if len(example1a)<100:
                        no_example1a.append([ X['LastDate'],CIK_t2])
                        no_example.Risk="Missing"
                    new=" ".join([example1,example1a])
                    CIK_h=X['CIK_File']
                    #holder = pd.DataFrame(np.array([CIK,X['Year'],X['SectionNew']['SectionText']['item1bus'],X['SectionNew']['SectionText']['item1ari'],new,X['SectionNew']['SectionText']['item7']]).reshape(1,6),columns=c)
                    holder = pd.DataFrame(np.array([CIK_h,   X['LastDate'],    X['Year'],        X['SectionNew']['SectionText']['item1bus'],       X['SectionNew']['SectionText']['item1ari']    ,new]).reshape(1,6),columns=c)
                    df1=df1.append(holder) 
                    
                    no_example_holder=no_example_holder.append(no_example)
    
                    save_file_name=windowsOrMac.join([intermediate_folder,"company_filings",str(CIK_t2)+"_10Ksections_timeseries.pkl"])
            except:
                print(str(CIK_t2)+"_"+str(int(os.listdir(of)[index3].split('_')[1]))+" Not Found")
        if len(df1)>0:
            df1.to_pickle(save_file_name)
        else:
            nada.append(CIK_t2)
    else:
        nofolder=nofolder+[CIK_t2]

no_example_holder.to_excel(excelname,sheet_name="Individual")
print(no_example1a)
print(no_example1)
#df1.Year
 

#index=os.listdir(datapath).index('0001332551-14-000009.txt')
Status_Holder[Status_Holder.Status=='Post2006Failure']