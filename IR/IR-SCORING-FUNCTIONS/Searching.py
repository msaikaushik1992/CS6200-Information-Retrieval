__author__ = 'saikaushik.mallela'


from pprint import pprint
import json
from stemming.porter2 import stem
from math import log

query_term_docid_tf = {}


#pprint(docno_to_docid_map_dictionary)
#
def average_length():
    sum = 0
    for keys in docno_to_docid_map_dictionary :
        for docid in docno_to_docid_map_dictionary[keys] :
            sum = sum + docno_to_docid_map_dictionary[keys][docid][3]
    average = float(sum)/len(docno_to_docid_map_dictionary)
    return average

#pprint(average_length())
# average = 1554.613823970524


def stopwords():
    file = open("AP89_DATA/AP_DATA/stoplist.txt")
    stop_words = []
    for line in file:
        if line:
            stop_words.append(line.rstrip('\n'))
    file.close()
    return stop_words

stopwords = stopwords()
#pprint(stopwords())
#pprint(stopwords)

# parsing query_desc.51-100.short.txt
##################################################
def querylist_without_duplicates():
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

#pprint(querylist_without_duplicates())


# creating a list without stoplist words
#################################################
def querylist_without_stoplist():

    querylist = []

    querylistwithout_duplicates = querylist_without_duplicates()
    querylist_withoutstoplist = [elem for elem in querylistwithout_duplicates if elem not in stopwords]
    #pprint(querylist_withoutstoplist)
    for q in querylist_withoutstoplist :
        q = q.lower()
        q = q.strip('!,;:#\-\"()\'')
        q = q.rstrip('.')
        q = q.rstrip(',')
        q = q.rstrip('\n')
        if '-' in q:
            for t in q.split('-'):
                if t in stopwords:
                    continue
                t = stem(t)
                querylist.append(t)
            continue
        q = stem(q)
        querylist.append(q)
    return querylist

#querylist_without_stoplist()

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
                        if t in stopwords: continue
                        new_lst.append(t)
                    continue
                #line = stem(line)
                new_lst.append(line)
        without_stoplist =[elem for elem in new_lst if elem not in stopwords]
        #pprint(without_stoplist)
        query_terms_without_stoplist = []
        for terms in without_stoplist:
            query_terms_without_stoplist.append(stem(terms))
        #print(query_terms_without_stoplist)
        each_query_line[qid] = query_terms_without_stoplist
        line = fob.readline()
    return each_query_line

#pprint(get_each_query())

all_words_in_all_queries = querylist_without_stoplist()
#storing a query term , doicid , termfrequency in a global dictionary query_term_docid_tf

#pprint(all_words_in_all_queries)

with open("final_catalog_with_stem_words_without_stop_words", "r") as f:
    catalog_dict = json.load(f)


#pprint(catalog_dict)

def retrieve_term_freq(term):
    doc_dict = {}
    list = catalog_dict[term]
    #pprint(list)
    offset = list[0]
    with open("final_inverted_index_with_stem_words_without_stop_words","r") as f:
        f.seek(offset)
        line = f.readline()
    #pprint(line)
    #pprint(line.split("#"))
    for doc in line.split("#")[1:] :
        document_id = doc.split(":")[0]
        data = doc.split(":")[1]
        data_list = data.split()
        tf = len(data_list)
        doc_dict[document_id] = tf
    return doc_dict

#pprint(retrieve_term_freq("hillberri"))


all_words_in_all_queries = querylist_without_stoplist()
#storing a query term , doicid , termfrequency in a global dictionary query_term_docid_tf
#######################################################

query_term_docid_tf = {}

def query_term_docid_tf_func():
    query_term_docid_tf_dummy ={}
    count = 0
    for word in all_words_in_all_queries:
        count += 1
        if word in catalog_dict:
            query_term_docid_tf_dummy[word] = retrieve_term_freq(word)
        else :
            continue
    return query_term_docid_tf_dummy

query_term_docid_tf = query_term_docid_tf_func()
#pprint(query_term_docid_tf)


query_term_ttf = {}

def retrieve_ttf_freq(term):
    ttf = 0
    query_term_ttf_dummy = {}
    dict = query_term_docid_tf[term]
    #pprint(dict)

    for keys in dict:
        ttf = ttf + dict[keys]
        pprint(ttf)
    query_term_ttf_dummy[term] = ttf
    return query_term_ttf_dummy


def query_term_ttf_func():
    query_term_ttf_dummy = {}
    count = 0

    for word in all_words_in_all_queries:
        count += 1
        print(count)
        if word in query_term_docid_tf:
            query_term_ttf_dummy = retrieve_ttf_freq(word)
        else :
            continue
    return query_term_ttf_dummy

query_term_ttf = query_term_ttf_func()
#pprint(query_term_ttf)

query_term_df ={}

def retrieve_df_freq(term):
    dict = query_term_docid_tf[term]
    df = len(dict)
    return df

def query_term_df_func():
    query_term_df_dummy = {}
    count = 0

    for word in all_words_in_all_queries:
        count += 1
        print(count)
        if word in query_term_docid_tf:
            query_term_df_dummy[word] = retrieve_df_freq(word)
        else:
            continue
    return query_term_df_dummy

query_term_df = query_term_df_func()

#pprint(query_term_df)

################################################   Get DOC ID Length  ###################################

def get_docids_dictionary():
    docids_dictionary = {}
    with open("docid_to_docnum", "r") as c:
        docids_dictionary = json.load(c)
    return docids_dictionary

docids_dictionary = get_docids_dictionary()
#pprint(docids_dictionary)

######################################################

#okapi okapi_tf(w,d)
def okapi_tf(tf,docid):
    #print("entered okapi_tf() for %s %s:" %(term, docid))
    pprint(docid)
    c = 0
    avg = 1554.613823970524
    dict  = docids_dictionary[docid]
    pprint(dict)
    for docnum in dict :
        c = float(docids_dictionary[docid][docnum][3])/avg
    denominator = tf + 0.5 + (1.5* c)
    okapi = tf/float(denominator)
    return okapi
############################################

# tf(d,q)

query_dict = get_each_query()

def okapi_tf_score(qid):
    #print("entered okapi_tf_score() for %s:" %(qid))
    #score_dict = {}
    doc_score ={}
    no_of_query_terms = 0
    list_of_query_terms = query_dict[qid]
    #print(list_of_query_terms)
    for terms in list_of_query_terms:
        if terms in query_term_docid_tf:
            dct= query_term_docid_tf[terms]
            for keys in dct:
                try:
                    tf = dct[keys]
                except KeyError:
                    tf = 0
                score = okapi_tf(tf,keys)
                if keys in doc_score:
                    doc_score[keys] += score
                else:
                    doc_score[keys] = score
    # returns a dictionary with key as docid's and value as score of that docid
    return doc_score
#######################################################

def get_qid():
    qid_list = []
    fob = open('AP89_DATA/AP_DATA/query_desc.51-100.short.txt','r+')
    line = fob.readline()
    #print(line)
    while(line):
        if line.split() != []:
            qid= line.split( )[0].rstrip('.')
            qid_list.append(qid)
        line = fob.readline()
    return qid_list
##################################################33

list_of_qid = get_qid()


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
        #fob.write(str(tup[0])+' '+'Q0'+' '+str(tup[1])+' '+str(okapi_result_list.index(tup)+1)+' '+str(tup[2])+' '+'Exp'+'\n')
        for docnum in docids_dictionary[tup[1]]:
            fob.write(str(tup[0])+' '+'Q0'+' '+str(docnum)+' '+str(okapi_result_list.index(tup)+1)+' '+str(tup[2])+' '+'Exp'+'\n')
    fob.close()


#okapi_results_file()


####################################################

def okapi_bm25(term, docid, qid) :
    D = len(docids_dictionary)
    try:
        dfw = query_term_df[term]
    except KeyError:
        dfw = 0
    print(dfw)
    first_term = log(float(D+0.5)/(dfw + 0.5))
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
    for docnum in docids_dictionary[docid]:
        second_term = (float(tf+(k1*tf))/(tf + k1*((1-b)+ (b*((docids_dictionary[docid][docnum][3])/1554.613823970524)))))
    third_term  = float(tfq+(k2*tfq))/(tfq+k2)
    okapi_bm25 = (first_term * second_term * third_term)
    return okapi_bm25

#####################################################

def okapi_bm25_score(qid) :
    doc_score = {}
    list_of_query_terms = query_dict[qid]
    for term in list_of_query_terms:
        if term in query_term_docid_tf:
            dct= query_term_docid_tf[term]
            for keys in dct:
                score = okapi_bm25(term,keys,qid)
                if keys in doc_score:
                    doc_score[keys] += score
                else:
                    doc_score[keys] = score
    return doc_score
######################################################

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
        #fob.write(str(tup[0])+' '+'Q0'+' '+str(tup[1])+' '+str(okapi_bm25_result_list.index(tup)+1)+' '+str(tup[2])+' '+'Exp'+'\n')
        for docnum in docids_dictionary[tup[1]]:
            fob.write(str(tup[0]) +' Q0 '+ str(docnum) +' '+str(okapi_bm25_result_list.index(tup)+1)  +' ' +str(tup[2]) +' Exp\n')
    fob.close()

#okapi_bm25_results_file()

######################################################

def lm_laplace(term,docid):
    print("entered lm_laplace")
    return log(p_laplace(term,docid))


def lm_laplace_score(qid) :
    doc_score = {}
    list_of_query_terms = query_dict[qid]
    for docid in docids_dictionary:
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
    for docnum in docids_dictionary[docid]:
        denominator = float(docids_dictionary[docid][docnum][3] + V)
    p_laplace = float(numerator)/denominator
    return p_laplace

def p_laplace_results_file():

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
        #fob.write(str(tup[0])+' '+'Q0'+' '+str(tup[1])+' '+str(lm_laplace_result_list.index(tup)+1)+' '+str(tup[2])+' '+'Exp'+'\n')
        for docnum in docids_dictionary[tup[1]]:
            fob.write(str(tup[0]) +' Q0 '+ str(docnum) +' '+str(lm_laplace_result_list.index(tup)+1)  +' ' +str(tup[2]) +' Exp\n')
    fob.close()

#########################################################

#p_laplace_results_file()

#########################################################Proximity Search ###########################

def get_least_range(query_position_list):
    number_of_terms = len(query_position_list)
    current_window_indicies = [0] * number_of_terms
    min_window_range = 10000000

    while True:
        window = []
        for term_number, index in enumerate(current_window_indicies):
            window.append(int(query_position_list[term_number][index]))

        window_range = max(window) - min(window)
        if window_range < min_window_range:
            min_window_range = window_range

        min_index = window.index(min(window))
        duplicate_window_list = window[:]
        while True:
            if len(query_position_list[min_index]) > current_window_indicies[min_index] + 1:
                current_window_indicies[min_index] += 1
                break
            else:
                duplicate_window_list.remove(min(duplicate_window_list))

                if len(duplicate_window_list) == 0:
                    return min_window_range

                min_index = window.index(min(duplicate_window_list))

def get_proximity_score(range,num_of_contain_terms,doc_len):
    c = 1500
    vocab = 155660
    numerator = (c - range) * num_of_contain_terms
    denominator = doc_len + vocab
    proximity = numerator/float(denominator)
    return proximity

def retrieve_docid_positions_dictionary(term):
    doc_dict = {}
    list = catalog_dict[term]
    #pprint(list)
    offset = list[0]
    with open("final_inverted_index_with_stem_words_without_stop_words","r") as f:
        f.seek(offset)
        line = f.readline()
    #pprint(line)
    #pprint(line.split("#"))
    for doc in line.split("#")[1:] :
        document_id = doc.split(":")[0]
        data = doc.split(":")[1]
        data_list = data.split()
        doc_dict[document_id] = data_list
    return doc_dict

query_term_docid_positions = {}

def query_term_docid_positions_func():
    query_term_docid_positions_dummy ={}
    count = 0
    for word in all_words_in_all_queries:
        count += 1
        if word in catalog_dict:
            query_term_docid_positions_dummy[word] = retrieve_docid_positions_dictionary(word)
        else :
            continue
    return query_term_docid_positions_dummy

query_term_docid_positions = query_term_docid_positions_func()


def run_proximity_search():
    query_dct = {}
    for key in query_dict:
        query_dct[key] = {}

        terms = query_dict[key]

        docids_with_query_terms = []
        for term in terms:
            if term in query_term_docid_positions:
                for docid in query_term_docid_positions[term]:
                    docids_with_query_terms.append(docid)

        for docid in docids_with_query_terms:
            query_position_list = []
            for term in terms:
                if term in query_term_docid_positions and docid in query_term_docid_positions[term]:
                    query_position_list.append(query_term_docid_positions[term][docid])
            if len(query_position_list) > 0:
                pprint(query_position_list)
                range = get_least_range(query_position_list)
                for docnum in docids_dictionary[docid]:
                    doc_length = docids_dictionary[docid][docnum][3]
                print(key,docid,range,len(query_position_list),doc_length)
                proximity_score = get_proximity_score(range,len(query_position_list),doc_length)
                query_dct[key][docid] = proximity_score

    f = open('proximity.txt', 'w+')
    lm_laplace_result_list = []
    for keys in query_dct:
        tup_list = []
        for dct in query_dct[keys]:
            tup = (keys,dct,query_dct[keys][dct])
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
    fob = open('proximity.txt','w+')
    for tup in lm_laplace_result_list:
        #fob.write(str(tup[0])+' '+'Q0'+' '+str(tup[1])+' '+str(lm_laplace_result_list.index(tup)+1)+' '+str(tup[2])+' '+'Exp'+'\n')
        for docnum in docids_dictionary[tup[1]]:
            fob.write(str(tup[0]) +' Q0 '+ str(docnum) +' '+str(lm_laplace_result_list.index(tup)+1)  +' ' +str(tup[2]) +' Exp\n')

    f.close()

run_proximity_search()

############################################################################################