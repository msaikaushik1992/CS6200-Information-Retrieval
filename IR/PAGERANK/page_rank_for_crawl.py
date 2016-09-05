__author__ = 'saikaushik.mallela'

import glob
import json
from pprint import pprint
from elasticsearch import Elasticsearch
from stemming.porter2 import stem
import math
import operator

page_rank = {}
outlink_count = {}
files = []
inlink_dict = {}
sink_nodes = []
newPR = {}

teleportation_factor = 0.85

#pdata = glob.glob('prashant/inlinks/*')
#sdata = glob.glob('sai/inlinks/*')
#rdata = glob.glob('ravi/inlinks/*')

#files.extend(pdata)
#files.extend(sdata)
#files.extend(rdata)

es = Elasticsearch("localhost:9200",timeout = 600,max_retries = 10,revival_delay = 0)
#pprint(files)
query = "CLIMATE CHANGE"
query = stem(query)

dict_of_inlinks_and_outlinks = {}
i_c = 0

docs = []

res = es.search(index="ass3_climatic_changes", body={"fields": ["_id"],"query": {"match_all":{}},"size":40000})

for key in res["hits"]["hits"]:
    url = key["_id"]
    docs = list(set(docs) | set([url]))

print(len(docs))

# pdata = glob.glob('prashant/inlinks/*')
# sdata = glob.glob('sai/inlinks/*')
# rdata = glob.glob('ravi/inlinks/*')
#
# files.extend(pdata)
# files.extend(sdata)
# files.extend(rdata)
#
# for file in files:
#     if file == 'prashant/inlinks\map_dict' or file == 'ravi/inlinks\map_dict' or file == 'sai/inlinks\map_dict' :
#         continue
#     fob = open(file,'r')
#     file_data = json.load(fob)
#     for url in file_data:
#         docs = list(set(docs) | set([url]))

for url in docs:
    res = es.search(index="ass3_climatic_changes", body={"query": {"match":{ "_id": url}}})
    doc = res['hits']['hits'][0]
    print(i_c)
    i_c = i_c + 1
    url = doc["_id"]
    inl = doc['_source']["in_links"]
    outl = doc['_source']["out_links"]
    if url in dict_of_inlinks_and_outlinks:
        new_in = set(dict_of_inlinks_and_outlinks[url][0]) | set(inl)
        new_out = set(dict_of_inlinks_and_outlinks[url][1]) | set(outl)
        dict_of_inlinks_and_outlinks[url] = (new_in,new_out)
    else:
        dict_of_inlinks_and_outlinks[url] = (inl,outl)
    # if url in inlink_dict:
    #     inlink_dict[url] = set(inlink_dict[url])| set(inl)
    # else:
    #     inlink_dict[url] = set(inl)

def get_all_links_and_populate_outlink_count_dict():
    some_list = []

    for url in dict_of_inlinks_and_outlinks:
        if url not in outlink_count:
            outlink_count[url] = 0
        for inlink in dict_of_inlinks_and_outlinks[url][0]:
            if inlink not in outlink_count:
                outlink_count[inlink] = 1
            else:
                outlink_count[inlink] += 1
        inlink_dict[url] = dict_of_inlinks_and_outlinks[url][0]
        some_list.append(url)
        #some_list += dict_of_inlinks_and_outlinks[url][0]
        print(len(some_list))
    get_all_links = set(some_list)
    return get_all_links


#
# def get_all_links_and_populate_outlink_count_dict():
#     some_list = []
#
#     for file in files:
#        if file == 'prashant/inlinks\map_dict' or file == 'ravi/inlinks\map_dict' or file == 'sai/inlinks\map_dict' :
#            continue
#        fob = open(file,'r')
#         dict_of_inlinks_and_outlinks = json.load(fob)
#         for url in dict_of_inlinks_and_outlinks:
#             if url not in outlink_count:
#                 outlink_count[url] = 0
#             for inlink in dict_of_inlinks_and_outlinks[url][0]:
#                 if inlink not in outlink_count:
#                     outlink_count[inlink] = 1
#                 else:
#                     outlink_count[url] += 1
#             inlink_dict = dict_of_inlinks_and_outlinks[url][0]
#             some_list.append(url)
#             some_list += dict_of_inlinks_and_outlinks[url][0]
#             print(len(some_list))
#         fob.close()
#         get_all_links = set(some_list)
#         return get_all_links

def initial_values(links):
    for link in links:
        page_rank[link] = 1/len(links)
    nooflinkes = len(links)
    return nooflinkes


def finding_sink_nodes():
    for link in outlink_count:
        if outlink_count[link] == 0:
            sink_nodes.append(link)

def calculate_perplexity():             # calculate perplexity for next iteration
    sum = 0
    for link in links:
        temp = page_rank[link]
        sum += temp * math.log(temp,2)
    return pow(2,(-1*sum))

def check_iteration_possible(old,new):
    old_floor = math.floor(old)
    new_floor = math.floor(new)
    old_floor_units_position = old_floor%10
    new_floor_units_position = new_floor%10
    if old_floor_units_position == new_floor_units_position:
        return 0
    else:
        return 1

def sort_write_dictionary():            # sort the final values and write them to HD
    final_list = []
    sorted_final_page_ranks = sorted(page_rank.items(), key=operator.itemgetter(1), reverse = True)
    id = 1
    for page in sorted_final_page_ranks:
        final_list.append(str(id) + ' ' +str(page[0]) + ' ' + str(page[1]) + '\n')
        id += 1
    page_count = 0
    with open('page_ranks_for_crawled_links_merged_team_index', 'w+') as f:
        for item in final_list:
            page_count += 1
            if page_count>500: break
            f.write(item)

def finding_page_rank():
    check = 0
    old_perplexity = 0.0
    flag =1
    while (flag):
        sinkPR = 0
        for link in sink_nodes:
            sinkPR += page_rank[link]
        for page in links:
            newPR[page] = (1-teleportation_factor)/number_of_links
            newPR[page] += teleportation_factor * sinkPR/number_of_links
            if page not in inlink_dict:
                continue
            for in_link in inlink_dict[page]:
                if in_link not in inlink_dict:
                    continue
                newPR[page] += teleportation_factor * (page_rank[in_link]/outlink_count[in_link])
        for url in links:
            page_rank[url] = newPR[url]
        new_perplexity = calculate_perplexity()
        print(new_perplexity)
        flag = check_iteration_possible(old_perplexity,new_perplexity)
        if flag == 0:
            check += 1
            if check == 4:
                flag = 0
            else:
                flag = 1
        else:
            check = 0
        old_perplexity = new_perplexity
    sort_write_dictionary()


if __name__ == '__main__':                       # main function
    links = get_all_links_and_populate_outlink_count_dict()
    number_of_links = initial_values(links)
    finding_sink_nodes()
    finding_page_rank()
