__author__ = 'saikaushik.mallela'

import glob
import json
from pprint import pprint
import math
import operator

page_rank = {}
outlink_count = {}
files = []
inlink_dict = {}
sink_nodes = []
newPR = {}

teleportation_factor = 0.85

pdata = glob.glob('prashant/inlinks/*')
sdata = glob.glob('sai/inlinks/*')
rdata = glob.glob('ravi/inlinks/*')

files.extend(pdata)
files.extend(sdata)
files.extend(rdata)

def get_all_links_and_populate_outlink_count_dict():
    some_list = []
    with open("wt2g_inlinks.txt",'r') as f:
        line = f.readline()
        while line:
            links = line.split()
            if links[0] not in outlink_count:
                outlink_count[links[0]] = 0
            for link in links[1:]:
                 if link not in outlink_count:
                     outlink_count[link] = 1
                 else:
                     outlink_count[link] += 1
            inlink_dict[links[0]] = links[1:]
            some_list += links
            line = f.readline()
    get_all_links = set(some_list)
    return get_all_links

def initial_values(links):
    for link in links:
        page_rank[link] = float (1/len(links))
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
    if old_floor == new_floor:
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
    with open('page_ranks_for_other_graph', 'w+') as f:
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
            newPR[page] = float((1-teleportation_factor))/number_of_links
            newPR[page] += float((teleportation_factor * sinkPR))/number_of_links
            if page not in inlink_dict:
                continue
            for in_link in inlink_dict[page]:
                newPR[page] += teleportation_factor * (page_rank[in_link]/outlink_count[in_link])
        for url in links:
            page_rank[url] = newPR[link]
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
