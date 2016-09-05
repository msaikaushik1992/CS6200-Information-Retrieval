__author__ = 'saikaushik.mallela'

from pprint import pprint
from elasticsearch import Elasticsearch
es = Elasticsearch("localhost:9200",timeout = 600,max_retries = 10,revival_delay = 0)
from stemming.porter2 import stem
import json
import operator
import math


#for groovy script
es.put_script(lang='groovy',id='gettf',body={'script': '_index[field][term].tf()'})
es.put_script(lang='groovy',id='getdf',body={'script': '_index[field][term].df()'})
es.put_script(lang='groovy',id='getttf',body={'script': '_index[field][term].ttf()'})

import glob
files = glob.glob("AP89_DATA/AP_DATA/ap89_collection/*")


#storing all docid's in a list named doclist
##################################################
doclist = []
fob = open('AP89_DATA/AP_DATA/doclist.txt','r')

line = fob.readline()
while line:
    if line.split( )[0] == '0':
        line = fob.readline()
        continue
    docid = line.split( )[1]
    doclist.append(docid)
    line = fob.readline()
total = len(doclist)
##################################################

# calculating length of each doc
#################################################
def length_of_each_doc(docid):
    length_of_eachdoc = 0
    terms = {}
    res = es.termvector(index="testindex",doc_type="document",id=docid,fields="text",term_statistics=True)
    try:
        terms = res['term_vectors']['text']['terms']
    except KeyError:
        length_of_eachdoc = 0
    for key in terms:
        term_freq = terms[key]['term_freq']
        length_of_eachdoc += term_freq
    return length_of_eachdoc
#################################################

# writing docid's to a file
#################################################

'''
docLengthDict={}
for docid in doclist :
    docLengthDict[docid] = length_of_each_doc(docid)

with open('docid1.txt','w+') as docLengthWriter:
    json.dump(docLengthDict,docLengthWriter)

with open('docid1.txt','r+') as docLengthReader:
    docLengthDict = json.load(docLengthReader)
'''

################################################

docid_dict = {}
def retrieving_docids_from_docid_file():
    fob = open("docid.txt","r")
    line = fob.readline()
    dociddict ={}
    while(line):
        docid = line.split()[0]
        line = line.split()[1]
        if line != '':
            dociddict[docid] = line
        line = fob.readline()
    return dociddict

#calculating average length of documents
###############################################

'''
def avg_length_of_docs():
    sum=0
    for docid in doclist:
        sum += length_of_each_doc(docid)
    return float(sum/len(doclist))

c = avg_length_of_docs();
#pprint(c);
'''


#c= 247.80769503294835



##############################################

#getting term frequency from elastic search using grrovy script
##############################################

def gettf(term):
    script_result = es.search(index='test_index',
                 body={
                     "query":
                     {
                         "match": {
                             "text": term
                         }
                     },
                     "script_fields": {
                         "tf": {
                             "script_id": "gettf",
                             "params": {"term": term, "field": "text"}
                         }
                     },
                     "size": 84769})
    return script_result
#pprint(gettf('predict'))
##################################################
#getting document frequency from elastic search using grrovy script

def getdf(term):
    script_result = es.search(index='test_index',
                 body={
                     "script_fields": {
                         "df": {
                             "script_id": "getdf",
                             "params": {"term": term, "field": "text"}
                         }
                     },
                     "size": 1})
    return script_result
#pprint(getdf('predict'))

# parsing stoplist.txt

##################################################

def getttf(term):
    script_result = es.search(index='test_index',
                 body={
                     "script_fields": {
                         "ttf": {
                             "script_id": "getttf",
                             "params": {"term": term, "field": "text"}
                         }
                     },
                     "size": 1})
    return script_result
#pprint(getttf('predict'))
##################################################
def get_terms_in_stoplist() :
    fob = open('AP89_DATA/AP_DATA/stoplist.txt','r')
    stoplist = []
    line = fob.readline()
    while(line):
        line = line.rstrip('\n')
        stoplist.append(line)
        line = fob.readline()
    return stoplist
#print(get_terms_in_stoplist())
##################################################
stoplist = get_terms_in_stoplist()

# parsing query_desc.51-100.short.txt
##################################################
def querylist_without_duplicates():
    #fob = open('AP89_DATA/AP_DATA/query_desc.51-100.short.txt','r')
    fob = open('AP89_DATA/AP_DATA/query_desc.51-100.short.txt','r')
    line = fob.readline()
    querylist = []
    count = 4
    while(line):
        if line.split( )[4:] == []:
            break
        querylist = querylist + line.split( )[4:]
        line = fob.readline()
    querylist_without_duplicates = set(querylist)
    return querylist_without_duplicates
##################################################

# creating a list without stoplist words
#################################################
def querylist_without_stoplist():

    querylist = []

    querylistwithout_duplicates = querylist_without_duplicates()
    querylist_withoutstoplist = [elem for elem in querylistwithout_duplicates if elem not in stoplist]
    for q in querylist_withoutstoplist :
        q = q.lower()
        q = q.strip('!,;:#\-\"()\'')
        q = q.rstrip('.')
        q = q.rstrip(',')
        q = q.rstrip('\n')
        if '-' in q:
            for t in q.split('-'):
                if t in stoplist:
                    continue
                t = stem(t)
                querylist.append(t)
            continue
        q = stem(q)
        querylist.append(q)
    return querylist

#################################################

#string docid's and term frequency in a dictionary
####################################################
def retrive_term_freq(term) :
    docid_dict = {}
    dct = gettf(term)
    listofdictionaries = dct['hits']['hits']
    for dcti in listofdictionaries:
        docid_dict[dcti['_id']] = dcti['fields']['tf'][0]
    return docid_dict
#pprint(retrive_term_freq('1,000'))
#######################################################

def retrieve_doc_freq(term) :
    term_doc_freq = 0
    dct = getdf(term)
    listofdictionaries = dct['hits']['hits']
    if listofdictionaries != []:
        term_doc_freq = listofdictionaries[0]['fields']['df'][0]
        #dfw = term_doc_freq[dcti['_id']]
    return term_doc_freq
#pprint(retrieve_doc_freq('predict'))

#####################################################

def retrieve_ttf_freq(term) :
    total_term_freq = 0.0
    dct = getttf(term)
    listofdictionaries = dct['hits']['hits']
    if listofdictionaries != []:
        total_term_freq = listofdictionaries[0]['fields']['ttf'][0]
    return total_term_freq
    # for dcti in listofdictionaries:
    #     term_doc_freq[dcti['_id']] = dcti['fields']['ttf'][0]
    #     #dfw = term_doc_freq[dcti['_id']]
    # return term_doc_freq
#pprint(retrieve_ttf_freq('predict'))

# getting query id and its corresponding line and storing them in a dictionary as key value pairs
query_dict = {}
qid_list = []
#################################################################
def get_each_query():
    fob = open('AP89_DATA/AP_DATA/query_desc.51-100.short.txt','r+')
    each_query_line = {}
    without_stoplist = []
    line = fob.readline()
    while line:
        if line.split() != []:
            qid= line.split( )[0].rstrip('.')
            line = line.split()[4:]
            new_lst = []
            for l in line:
                line = l.strip('!,;:#\-\"()\'').rstrip(',').rstrip('.')
                line = line.lower()
                if '-' in line:
                    for t in line.split('-'):
                        if t in stoplist: continue
                        new_lst.append(t)
                    continue
                #line = stem(line)
                new_lst.append(line)
        without_stoplist =[elem for elem in new_lst if elem not in stoplist]
        #pprint(without_stoplist)
        query_terms_without_stoplist = []
        for terms in without_stoplist:
            query_terms_without_stoplist.append(stem(terms))
        #print(query_terms_without_stoplist)
        each_query_line[qid] = query_terms_without_stoplist
        line = fob.readline()
    return each_query_line
#get_each_query()

all_words_in_all_queries = querylist_without_stoplist()
#storing a query term , doicid , termfrequency in a global dictionary query_term_docid_tf
#######################################################

query_term_docid_tf = {}

def query_term_docid_tf_func():
    count = 0

    for word in all_words_in_all_queries:
        count += 1
        print(count)
        query_term_docid_tf[word] = retrive_term_freq(word)
    return query_term_docid_tf

##################################################

query_term_df = {}

def query_term_df_func():
    count = 0
    query_term_df = {}
    for word in all_words_in_all_queries:
        count += 1
        print(count)
        query_term_df[word] = retrieve_doc_freq(word)
    return query_term_df

#pprint(query_term_df_func())

################################################

def query_term_ttf_func():
    count = 0
    query_term_ttf = {}
    for word in all_words_in_all_queries:
        count += 1
        print(count)
        query_term_ttf[word] = retrieve_ttf_freq(word)
    return query_term_ttf

#pprint(query_term_ttf_func())

##############################################
#getting all query id's and append to a list to call okapi_tf_score(qid) on this list
qid_list = []
#################################################################
def get_qid():
    fob = open('AP89_DATA/AP_DATA/query_desc.51-100.short.txt','r+')
    line = fob.readline()
    #print(line)
    while(line):
        if line.split() != []:
            qid= line.split( )[0].rstrip('.')
            qid_list.append(qid)
        line = fob.readline()
    return qid_list

######################################################

#okapi okapi_tf(w,d)
def okapi_tf(term,docid):
    #print("entered okapi_tf() for %s %s:" %(term, docid))
    avg = 248
    length_doc = float(docid_dict[docid])
    try:
        tf = query_term_docid_tf[term][docid]
    except KeyError:
        tf =  0
    c = length_doc/float(avg)
    denominator = tf + 0.5 + (1.5* c)
    okapi = tf/float(denominator)
    return okapi
############################################

# tf(d,q)

def okapi_tf_score(qid):
    #print("entered okapi_tf_score() for %s:" %(qid))
    #score_dict = {}
    doc_score ={}
    no_of_query_terms = 0
    list_of_query_terms = query_dict[qid]
    #print(list_of_query_terms)
    for term in list_of_query_terms:
        dct= query_term_docid_tf[term]
        for keys in dct:
            score = okapi_tf(term,keys)
            if keys in doc_score:
                doc_score[keys] += score
            else:
                doc_score[keys] = score
    # returns a dictionary with key as docid's and value as score of that docid
    return doc_score
#######################################################

def tfidf(term,docid):
    #print("entered okapi_tf() for %s %s:" %(term, docid))
    avg = 248
    length_doc = float(docid_dict[docid])
    D = len(doclist)
    try:
        dfw = query_term_df[term]
    except KeyError:
        dfw =0
    '''
    if term in query_term_docid_tf:
        dct =  query_term_docid_tf[term]
        for keys in dct :
            dfw += 1
    print(dfw)
    '''
    try:
        tf = query_term_docid_tf[term][docid]
    except KeyError:
        tf =  0
    c = length_doc/float(avg)
    denominator = tf + 0.5 + (1.5* c)
    okapi = tf/float(denominator)
    tfidf =  okapi*math.log(float(D)/dfw)
    return tfidf

#############################################

def tfidf_score(qid):
    #print("entered okapi_tf_score() for %s:" %(qid))
    #score_dict = {}
    doc_score ={}
    no_of_query_terms = 0
    list_of_query_terms = query_dict[qid]
    #print(list_of_query_terms)
    for term in list_of_query_terms:
        dct= query_term_docid_tf[term]
        for keys in dct:
            score = tfidf(term,keys)
            if keys in doc_score:
                doc_score[keys] += score
            else:
                doc_score[keys] = score
    # returns a dictionary with key as docid's and value as score of that docid
    return doc_score
####################################################

def okapi_bm25(term, docid, qid) :
    D = len(doclist)
    try:
        dfw = query_term_df[term]
    except KeyError:
        dfw = 0
    print(dfw)
    first_term = math.log(float(D+0.5)/(dfw + 0.5))
    try:
        tf = query_term_docid_tf[term][docid]
    except KeyError:
        tf = 0
    k1= 1.2
    b = 0.75
    k2 = 1000
    tfq = 1
    list = query_dict[qid]
    for l in list:
        if l == term:
            tfq+=1
    second_term = (float(tf+(k1*tf))/(tf + k1*((1-b)+ (b*((float(docid_dict[docid]))/248)))))
    third_term  = float(tfq+(k2*tfq))/(tfq+k2)
    okapi_bm25 = (first_term * second_term * third_term)
    return okapi_bm25

#####################################################

def okapi_bm25_score(qid) :
    doc_score = {}
    list_of_query_terms = query_dict[qid]
    for term in list_of_query_terms:
        dct= query_term_docid_tf[term]
        for keys in dct:
            score = okapi_bm25(term,keys,qid)
            if keys in doc_score:
                doc_score[keys] += score
            else:
                doc_score[keys] = score
    return doc_score
#####################################################
# global dictionaries
query_dict = get_each_query()
for keys in query_dict:
    pprint(keys)
docid_dict = retrieving_docids_from_docid_file()
#pprint(docid_dict)
query_term_docid_tf = query_term_docid_tf_func()
#pprint(query_term_docid_tf)
query_term_df = query_term_df_func()
#pprint(query_term_df)
query_term_ttf = query_term_ttf_func()
#print(query_term_ttf)
#global lists

#print(all_words_in_all_queries)
list_of_qid = get_qid()
#print(list_of_qid)

def okapi_bm25_results_file():
    scores={}

    for qids in list_of_qid:
        scores[qids] = okapi_bm25_score(qids)
    # this loop above creates a dictionary within a dictionary with outer dictionary key as qid and its value
    # a dictionary whose key value pairs are docid's and scores
    okapi_bm25_result_list = []
    for keys in scores:
        tup_list = []
        for dct in scores[keys]:
            tup = (keys,dct,scores[keys][dct])
            tup_list.append(tup)
    # this for loop creates a list of tuples with each tuple having [qid,docid,score]
        tup_list = sorted(tup_list, key=lambda tup : tup[2],reverse=True)
        tup_list = tup_list[:99]
        new_list = []
        for t in tup_list:
            if tup[2] != 0:
                new_list.append(t)
        okapi_bm25_result_list.extend(new_list)

    #writng the desired output format to result_okapi.txt
    fob = open('result_bm25.txt','w+')
    for tup in okapi_bm25_result_list:
        fob.write(str(tup[0])+' '+'Q0'+' '+str(tup[1])+' '+str(okapi_bm25_result_list.index(tup)+1)+' '+str(tup[2])+' '+'Exp'+'\n')
    fob.close()
#############################################################

##############################################


def okapi_results_file():

    scores={}

    for qids in list_of_qid:
        scores[qids] = okapi_tf_score(qids)
    # this loop above creates a dictionary within a dictionary with outer dictionary key as qid and its value
    # a dictionary whose key value pairs are docid's and scores
    okapi_result_list = []
    for keys in scores:
        tup_list = []
        for dct in scores[keys]:
            tup = (keys,dct,scores[keys][dct])
            tup_list.append(tup)
    # this for loop creates a list of tuples with each tuple having [qid,docid,score]
        tup_list = sorted(tup_list, key=lambda tup : tup[2],reverse=True)
        tup_list = tup_list[:99]
        new_list = []
        for t in tup_list:
            if tup[2] != 0:
                new_list.append(t)
        okapi_result_list.extend(new_list)

    #writng the desired output format to result_okapi.txt
    fob = open('result_okapi.txt','w+')
    for tup in okapi_result_list:
        fob.write(str(tup[0])+' '+'Q0'+' '+str(tup[1])+' '+str(okapi_result_list.index(tup)+1)+' '+str(tup[2])+' '+'Exp'+'\n')
    fob.close()
#############################################################################

def tfidf_results_file():

    scores={}

    for qids in list_of_qid:
        scores[qids] = tfidf_score(qids)
    # this loop above creates a dictionary within a dictionary with outer dictionary key as qid and its value
    # a dictionary whose key value pairs are docid's and scores
    tfidf_result_list = []
    for keys in scores:
        tup_list = []
        for dct in scores[keys]:
            tup = (keys,dct,scores[keys][dct])
            tup_list.append(tup)
    # this for loop creates a list of tuples with each tuple having [qid,docid,score]
        tup_list = sorted(tup_list, key=lambda tup : tup[2],reverse=True)
        tup_list = tup_list[:99]
        new_list = []
        for t in tup_list:
            if tup[2] != 0:
                new_list.append(t)
        tfidf_result_list.extend(new_list)

    #writng the desired output format to result_okapi.txt
    fob = open('result_tfidf.txt','w+')
    for tup in tfidf_result_list:
        fob.write(str(tup[0])+' '+'Q0'+' '+str(tup[1])+' '+str(tfidf_result_list.index(tup)+1)+' '+str(tup[2])+' '+'Exp'+'\n')
    fob.close()

######################################################

def lm_laplace(term,docid):
    print("entered lm_laplace")
    return math.log(p_laplace(term,docid))


def lm_laplace_score(qid) :
    doc_score = {}
    list_of_query_terms = query_dict[qid]
    for docid in doclist:
        for term in list_of_query_terms:
            score = lm_laplace(term,docid)
            if docid in doc_score:
                doc_score[docid] += score
            else:
                doc_score[docid] = score
    return doc_score

def p_laplace(term,docid):
    print("entered p_laplace(term,docid) ")
    try:
        tf = query_term_docid_tf[term][docid]
    except KeyError:
        tf =  0
    numerator = tf + 1
    V = 178050
    denominator = float(docid_dict[docid]) + V
    p_laplace = float(numerator)/denominator
    return p_laplace

def lm_laplace_results_file():

    scores={}

    for qids in list_of_qid:
        scores[qids] = lm_laplace_score(qids)
    # this loop above creates a dictionary within a dictionary with outer dictionary key as qid and its value
    # a dictionary whose key value pairs are docid's and scores
    lm_laplace_result_list = []
    for keys in scores:
        tup_list = []
        for dct in scores[keys]:
            tup = (keys,dct,scores[keys][dct])
            tup_list.append(tup)
    # this for loop creates a list of tuples with each tuple having [qid,docid,score]
        tup_list = sorted(tup_list, key=lambda tup : tup[2],reverse=True)
        tup_list = tup_list[:99]
        new_list = []
        for t in tup_list:
            if tup[2] != 0:
                new_list.append(t)
        lm_laplace_result_list.extend(new_list)

    #writng the desired output format to result_okapi.txt
    fob = open('result_laplace.txt','w+')
    for tup in lm_laplace_result_list:
        fob.write(str(tup[0])+' '+'Q0'+' '+str(tup[1])+' '+str(lm_laplace_result_list.index(tup)+1)+' '+str(tup[2])+' '+'Exp'+'\n')
    fob.close()

#########################################################

def total_doc_length():
    length = 0
    for docid in doclist:
        length +=  float(docid_dict[docid])
    return length

total_doc_length = total_doc_length()

def p_jm(term,docid):
    lamda = 0.55
    try:
        tf = query_term_docid_tf[term][docid]
    except KeyError:
        tf =  0
    length = float(docid_dict[docid])
    first_term = 0
    print("tf:" + str(tf))
    if not length == 0 :
        first_term = (lamda * (tf/float(length)))
    print("first_term:" + str(first_term))
    try:
        ttf = query_term_ttf[term]
    except KeyError:
        ttf = 0
    print("ttf:" + str(ttf))
    try:
        tf = query_term_docid_tf[term][docid]
    except KeyError:
        tf = 0
    second_term_numerator = ttf - tf
    second_term_denominator = total_doc_length - float(length)
    second_term = (1-lamda) * float(second_term_numerator)/second_term_denominator
    print("second-term: " + str(second_term))
    p_jm = first_term + second_term
    return p_jm

def lm_jm(term,docid):
    print("entered lm_jm")
    c = p_jm(term,docid)
    if not c == 0:
        return math.log(c)
    else:
        return 0


def lm_jm_score(qid) :
    doc_score = {}
    score = 0
    list_of_query_terms = query_dict[qid]
    for docid in doclist:
        for term in list_of_query_terms:
            score = lm_jm(term,docid)
            if docid in doc_score:
                doc_score[docid] += score
            else:
                doc_score[docid] = score
            print(score)
    return doc_score

#lm_jm_score('77')

#pprint(p_jm('job','AP890212-0035'))

def jm_results_file():

    scores={}

    for qids in list_of_qid:
        scores[qids] = lm_jm_score(qids)
    # this loop above creates a dictionary within a dictionary with outer dictionary key as qid and its value
    # a dictionary whose key value pairs are docid's and scores
    lm_jm_result_list = []
    for keys in scores:
        tup_list = []
        for dct in scores[keys]:
            tup = (keys,dct,scores[keys][dct])
            tup_list.append(tup)
    # this for loop creates a list of tuples with each tuple having [qid,docid,score]
        tup_list = sorted(tup_list, key=lambda tup : tup[2],reverse=True)
        tup_list = tup_list[:99]
        new_list = []
        for t in tup_list:
            if tup[2] != 0:
                new_list.append(t)
        lm_jm_result_list.extend(new_list)

    #writng the desired output format to result_okapi.txt
    fob = open('result_jm.txt','w+')
    for tup in lm_jm_result_list:
        fob.write(str(tup[0])+' '+'Q0'+' '+str(tup[1])+' '+str(lm_jm_result_list.index(tup)+1)+' '+str(tup[2])+' '+'Exp'+'\n')
    fob.close()

#pprint(total_doc_length)
########################################################
# for okapi_tf vector space model:

#okapi_results_file()

# for tf-idf vector space model:

#tfidf_results_file()

# for Okapi BM25

#okapi_bm25_results_file()

# for Unigram LM with Laplace smoothing

#lm_laplace_results_file()

# Unigram LM with Jelinek-Mercer smoothing

jm_results_file()

'''
def avg():
    length = 0
    for docid in docid_dict:
        length += float(docid_dict[docid])
    return float(length)/len(docid_dict)

pprint(avg())
'''