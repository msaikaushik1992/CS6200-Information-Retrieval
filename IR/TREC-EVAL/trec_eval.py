__author__ = 'saikaushik.mallela'


import math
from collections import Counter
from pprint  import pprint
import sys

############################### declaring variables and dictionaries ###########################


qrel_file = ""
ranked_list_file = ""
sum_prec_at_cutoffs = []
sum_rec_at_cutoffs = []
sum_prec_at_recalls = []
sum_avg_prec = 0
sum_r_prec = 0
avg_prec_at_cutoffs = []
avg_prec_at_recalls = []
avg_rec_at_cutoffs = []
tot_num_ret = 0
tot_num_rel = 0
tot_num_rel_ret = 0


number_of_relevant_documents_count = {}

qrel_dictionary = {}

qrel_for_dcg = {}


####################### getting input to the program ##############################


if len(sys.argv) < 3 and len(sys.argv) > 4:
    print("Give correct input....")
    exit()
elif len(sys.argv) == 4:
    print_all_queries = True
    qrel_file = sys.argv[2]
    trec_file = sys.argv[3]
elif len(sys.argv) == 3:
    print_all_queries = False
    qrel_file = sys.argv[1]
    trec_file = sys.argv[2]

with open(qrel_file, 'r') as q:
    qrel = q.readlines()

with open(trec_file, 'r') as r:
    trec = r.readlines()

#####################################build qrel dictionary##########################
qrel_dict_temp = {}


for line in qrel:
    line.replace("\n","")
    line_as_list = line.split(' ')

    qid = int(line_as_list[0])
    accessorid = line_as_list[1]
    docid = line_as_list[2]
    grade = int(line_as_list[3])

    if qid in qrel_dict_temp:
        temp = qrel_dict_temp[qid]
        if docid in temp:
            temp[docid].append(grade)
        else:
            temp[docid] = [grade]
        qrel_dict_temp[qid] = temp
    else:
        temp = {}
        temp[docid] = [grade]
        qrel_dict_temp[qid] = temp

#pprint(qrel_dict_temp)

####### logic for  getting final grade value depending on the assumptions ##############################

for qid in qrel_dict_temp:
    for docid in qrel_dict_temp[qid]:

        combined_grade = None

        scores_as_list = qrel_dict_temp[qid][docid]
        count_of_list_of_key_value_tuples = Counter(scores_as_list).items()
        # choose the most repeated grade
        for tuple in count_of_list_of_key_value_tuples:
            if tuple[1] > 1: score = tuple[0]

        # if not repeated, take the average
        if combined_grade == None:
            if 0 in scores_as_list and 2 in scores_as_list:
                combined_grade = 1

        # else take maximum
        if combined_grade == None:
            combined_grade = max(scores_as_list)

        if qid in qrel_for_dcg:
            qrel_for_dcg[qid].append(combined_grade)
        else:
            qrel_for_dcg[qid] = [combined_grade]
        #pprint(len(qrel_for_dcg[qid]))

        if combined_grade >= 1:
            relevance = 1
        else:
            relevance = 0

        if qid in number_of_relevant_documents_count:
            number_of_relevant_documents_count[qid] += relevance
        else:
            number_of_relevant_documents_count[qid] = relevance


        if qid in qrel_dictionary:
            temp = {}
            temp[docid] = relevance
            qrel_dictionary[qid].update(temp)
        else:
            temp = {}
            temp[docid] = relevance
            qrel_dictionary[qid] = temp
        #pprint(len(qrel_dictionary[qid]))

#pprint(number_of_relevant_documents_count)
###################################################################################

#pprint(qrel_for_dcg)
#pprint(qrel_dictionary)

############################################Build TREC Dictionary##################################
trec_dictionary = {}

for line in trec:
    line.replace("\n","")
    line_as_list = line.split(' ')
    qid = int(line_as_list[0])
    docid = line_as_list[2]
    score = float(line_as_list[4])

    if qid in trec_dictionary:
        temp = {}
        temp[docid] = score
        trec_dictionary[qid].update(temp)
    else:
        temp = {}
        temp[docid] = score
        trec_dictionary[qid] = temp

#pprint(trec_dictionary)

############################################ Print ##################################

def eval_print(qid,ret,rel,rel_ret,avg_prec_at_recalls,map,avg_prec_at_cutoffs,avg_rec_at_cutoffs,rp):
    print("Queryid (Num):    ",qid)
    print("Total number of documents over all queries")
    print("    Retrieved:  ",ret)
    print("    Relevant:   ",rel)
    print("    Rel_ret:    ",rel_ret)
    print("Interpolated Recall - Precision Averages:")

    ## printing recals
    for i in range(0,len(avg_prec_at_recalls)):
        print("    at ",recall[i],"       ",round(avg_prec_at_recalls[i],4))

    print("Average precision (non-interpolated) for all rel docs(averaged over queries)")
    print("                  ",round(map,4))
    print("Precision:")

    for i in range(0,len(avg_prec_at_cutoffs)):
        print("  At    ",cutoffs[i]," docs:   ",round(avg_prec_at_cutoffs[i],4))

    print("Recalls:")

    for i in range(0,len(avg_rec_at_cutoffs)):
        print("  At    ",cutoffs[i]," docs:   ",round(avg_rec_at_cutoffs[i],4))

    print("R-Precision (precision after R (= number_of_relevant_documents_count for a query) docs retrieved):")
    print("    Exact:        ",round(rp,4))

    print("F1 Measure:")
    for i in range(0,len(avg_prec_at_cutoffs)):
        if avg_prec_at_cutoffs[i] != 0 and avg_rec_at_cutoffs[i] != 0:
            average_f_measure = float((2 * avg_prec_at_cutoffs[i] * avg_rec_at_cutoffs[i])) / float(avg_prec_at_cutoffs[i] + avg_rec_at_cutoffs[i])
        else:
            average_f_measure = 0.0
        print("    at ",cutoffs[i],"       ",round(average_f_measure,4))
    print("\n")

###########################################################################################################

recall =  [0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0]

cutoffs = [5,10, 20, 50, 100]

number_of_queries = 0

qids = []

for qid in trec_dictionary:
    if qid not in qids:
        qids.append(qid)

qids.sort()

#pprint(qids)

for qid in qids:
    if not number_of_relevant_documents_count:
        continue

    number_of_queries += 1

    docid_score_dict = trec_dictionary[qid]

    precision_list = []
    for i in range(0,1001):
        precision_list.append(0)
    recall_list = []
    for i in range(0,1001):
        recall_list.append(0)

    number_of_retrieved_documents = 0
    number_of_relevant_documents_retrieved = 0
    sum_precision = 0

    list_of_docid_score_tuples = []
    for docid in docid_score_dict:
        score = docid_score_dict[docid]
        tup = (docid,score)
        list_of_docid_score_tuples.append(tup)
        del tup
    sorted_doc_as_tupples = sorted(list_of_docid_score_tuples, key=lambda tup: tup[1], reverse=True)


    for data in sorted_doc_as_tupples:

        number_of_retrieved_documents += 1

        docid = data[0]

        #pprint(docid)
        score = data[1]

        try:
            rel = qrel_dictionary[qid][docid]
        except:
            rel = 0

        if rel != 0:
            sum_precision += (relevance * (1 + number_of_relevant_documents_retrieved)) / float(number_of_retrieved_documents)
            number_of_relevant_documents_retrieved += relevance

        precision_list[number_of_retrieved_documents] =  number_of_relevant_documents_retrieved/float(number_of_retrieved_documents)

        recall_list[number_of_retrieved_documents] =  number_of_relevant_documents_retrieved/float(number_of_relevant_documents_count[qid])

        if number_of_retrieved_documents > 1000:
            break

    average_precision = sum_precision/float(number_of_relevant_documents_count[qid])

    # Fill out the remainder of the precision/recall lists, if necessary.

    final_recall = float(number_of_relevant_documents_retrieved)/float(number_of_relevant_documents_count[qid])


    for i in range(number_of_retrieved_documents+1,1001):
        precision_list[i] = number_of_relevant_documents_retrieved/float(i)
        recall_list[i] = final_recall

    # Now calculate precision at document cutoff levels and R-precision.
    # Note that arrays are indexed starting at 0...

    precision_at_cutoffs = []

    for cutoff in cutoffs:
        precision_at_cutoffs.append(precision_list[cutoff])

    #pprint(precision_at_cutoffs)

    recall_at_cutoffs = []
    for cutoff in cutoffs:
        recall_at_cutoffs.append(recall_list[cutoff])

    # Now calculate R-precision.  We'll be a bit anal here and
    # actually interpolate if the number of relevant docs is not
    # an integer...

    if number_of_relevant_documents_count[qid] > number_of_retrieved_documents:
        R_precision = number_of_relevant_documents_retrieved/float(number_of_relevant_documents_count[qid])
    else:
        integer_part_of_number_of_relevant_documents_retrieved = int(number_of_relevant_documents_count[qid])
        fractional_part_of_number_of_relevant_documents_retrieved = number_of_relevant_documents_count[qid] - integer_part_of_number_of_relevant_documents_retrieved     # Fractional part.

        if fractional_part_of_number_of_relevant_documents_retrieved > 0:
            R_precision = (1 - fractional_part_of_number_of_relevant_documents_retrieved) * precision_list[integer_part_of_number_of_relevant_documents_retrieved] + fractional_part_of_number_of_relevant_documents_retrieved * precision_list[integer_part_of_number_of_relevant_documents_retrieved+1]
        else:
            R_precision = precision_list[integer_part_of_number_of_relevant_documents_retrieved]

    # Now calculate interpolated precisions...

    max_prec = 0
    for i in range(1000,0,-1):
        if precision_list[i] > max_prec:
            max_prec = precision_list[i]
        else:
            precision_list[i] = max_prec

    # Finally, calculate precision at recall levels.

    prec_at_recalls = []

    i = 1
    for r in recall:
        while (i <= 1000 and recall_list[i] < r):
            i += 1
        if (i <= 1000):
            prec_at_recalls.append(precision_list[i])
        else:
            prec_at_recalls.append(0)

    # Print stats on a per query basis if requested.

    if print_all_queries:
        eval_print(qid, number_of_retrieved_documents, number_of_relevant_documents_count[qid], number_of_relevant_documents_retrieved,
                   prec_at_recalls, average_precision, precision_at_cutoffs,recall_at_cutoffs, R_precision)

    # Now update running sums for overall stats.

    tot_num_ret += number_of_retrieved_documents
    tot_num_rel += number_of_relevant_documents_count[qid]
    tot_num_rel_ret += number_of_relevant_documents_retrieved

    for i in range(0,len(cutoffs)):
        if i < len(sum_prec_at_cutoffs):
            sum_prec_at_cutoffs[i] += precision_at_cutoffs[i]
        else:
            sum_prec_at_cutoffs.append(precision_at_cutoffs[i])


    for i in range(0,len(cutoffs)):
        if i < len(sum_rec_at_cutoffs):
            sum_rec_at_cutoffs[i] += recall_at_cutoffs[i]
        else:
            sum_rec_at_cutoffs.append(recall_at_cutoffs[i])

    for i in range(0,len(recall)):
        if i < len(sum_prec_at_recalls):
            sum_prec_at_recalls[i] += prec_at_recalls[i]
        else:
            sum_prec_at_recalls.append(prec_at_recalls[i])

    sum_avg_prec += average_precision
    sum_r_prec += R_precision

# pprint(precision_at_cutoffs)

# Now calculate summary stats.

for i in range(0,len(cutoffs)):
    if i < len(avg_prec_at_cutoffs):
        avg_prec_at_cutoffs[i] = sum_prec_at_cutoffs[i]/float(number_of_queries)
    else:
        avg_prec_at_cutoffs.append(sum_prec_at_cutoffs[i]/float(number_of_queries))

for i in range(0,len(cutoffs)):
    if i < len(avg_rec_at_cutoffs):
        avg_rec_at_cutoffs[i] = sum_rec_at_cutoffs[i]/float(number_of_queries)
    else:
        avg_rec_at_cutoffs.append(sum_rec_at_cutoffs[i]/float(number_of_queries))

for i in range(0,len(recall)):
    if i < len(avg_prec_at_recalls):
        avg_prec_at_recalls[i] = sum_prec_at_recalls[i]/float(number_of_queries)
    else:
        avg_prec_at_recalls.append(sum_prec_at_recalls[i]/float(number_of_queries))

mean_avg_prec = sum_avg_prec/float(number_of_queries)
avg_r_prec = sum_r_prec/float(number_of_queries)

eval_print(number_of_queries, tot_num_ret, tot_num_rel, tot_num_rel_ret,avg_prec_at_recalls, mean_avg_prec, avg_prec_at_cutoffs,
           avg_rec_at_cutoffs, avg_r_prec)

########################### NDCG #####################
r_vector = []
dcg_dict = {}

sum_ndcg = 0.0

for q_id in qrel_for_dcg:
    r_vector = qrel_for_dcg[q_id]
    dcg = r_vector[0]

    for i in range(1,len(r_vector)):
        dcg += (float(r_vector[i])/math.log(i+1))

    r_vector.sort(reverse=True)
    sorted_dcg = r_vector[0]
    if sorted_dcg > 0:
        for i in range(1, len(r_vector)):
            sorted_dcg += (float(r_vector[i])/math.log(i+1))
        ndcg = float(dcg)/sorted_dcg
    else:
        ndcg = 0.0

    dcg_dict[q_id] = ndcg
    sum_ndcg += ndcg

ndcg_score = sum_ndcg/float(len(qrel_for_dcg))
print("nDCG:" + "\t",round(ndcg_score, 4))






