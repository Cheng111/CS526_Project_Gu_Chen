#!/opt/anaconda3/bin/python

import pandas as pd
import networkx as nx
from pyspark import SparkContext
import time
import numpy as np
import gdown
import os

fdownload = {
"large.clique" : "https://drive.google.com/file/d/1BKjc9we7qCoJ5lFAoY5DCYXxbZIrNG4A/view?usp=sharing",
"middle.clique" : "https://drive.google.com/file/d/1i24rvEufCwibeDh9RzqUEigZHxSv4KcG/view?usp=sharing",
"small.clique" : "https://drive.google.com/file/d/1phbG6V8Mx2Dk30rfAL2gI0xYCZIYhlHc/view?usp=sharing",
"C14.280.edgelist": "https://drive.google.com/file/d/1p7wPv4CxtGJ9y8GQ7kXsNpM89xxwbpPE/view?usp=sharing",
"C14-280-cliques.clique": "https://drive.google.com/file/d/1OZTfCtZpvNXvQjEpWjZjNhCm01eqbBSd/view?usp=sharing"}

for output, url in fdownload.items():
    gdown.download(url, output, quiet=False, fuzzy=True)

graphfile = "C14.280.edgelist"
graph = pd.read_csv(graphfile, header = None)
graph = graph.iloc[1: , :]
graph = graph[0].str.split(expand=True)
graph = nx.from_pandas_edgelist(graph, source = 0, target = 1)


smallclique = "small.clique"
middleclique = "middle.clique"
largeclique = "large.clique"


sep = "\t"
glom = 1


#without MapReduce
def para_clique_NMR(clique, sep, G, glom):
    cnodes = set(clique.strip().split(sep))
    len_cnodes = len(cnodes)
    neibors = {}
    for cnode in cnodes:
        cnode_neibors = set(G.neighbors(cnode)) - cnodes
        for cneibor in cnode_neibors:
            if cneibor not in neibors.keys():
                neibors[cneibor] = 0
            neibors[cneibor] = neibors[cneibor] + 1
    neibors = dict(filter(lambda x: x[1] + glom >= len_cnodes, neibors.items()))
    res_edges = []
    for impute_node, glom_num in neibors.items():
        impute_clique_nodes = cnodes - set(G.neighbors(impute_node))
        for clique_node in impute_clique_nodes:
            res_edges.append((impute_node, clique_node))
    return res_edges


def paraclique1(cliques, sep, graph, glom):
    impute_edge = []
    cliques["res"] = cliques["cliques"].apply(para_clique_NMR, args = (sep, graph, glom))
    for i in cliques.index:
        impute_edge.extend(cliques.loc[i, "res"])
    return impute_edge


def para_clique_FMR(clique, sep, G, glom):
    cnodes = set(clique.strip().split(sep))
    len_cnodes = len(cnodes)
    neibors = {}
    neibors_source = {}
    # find all the neibors for all the nodes in the clique
    for cnode in cnodes:
        cnode_neibors = set(G.neighbors(cnode))
        cnode_neibors = cnode_neibors - cnodes
        for cneibor in cnode_neibors:
            if cneibor not in neibors.keys():
                neibors[cneibor] = 0
                neibors_source[cneibor] = set([])
            neibors[cneibor] = neibors[cneibor] + 1
            neibors_source[cneibor].add(cnode)
    res = []
    for node, num in neibors.items():
        if num + glom >= len_cnodes:
            res.append( (node, len_cnodes - num) )
    #res_edges = set([])
    res_edges = []
    for impute_node, glom_num in res:
        impute_clique_nodes = cnodes - neibors_source[impute_node]
        for clique_node in impute_clique_nodes:
            res_edges.append((impute_node, clique_node))
    return res_edges


def paraclique2(sc, cliques, sep, graph, glom):
    cliques = sc.parallelize(cliques.values)
    impute_edges = cliques.flatMap(lambda x: para_clique_FMR(x[0], sep, graph, glom))
    impute_edges = impute_edges.collect()
    return impute_edges


def cliques_tonode(clique, sep):
    ID = [clique[0]] * len(clique[1])
    return list(zip(ID, clique[1]))
def process_node(clique_id, cnode, cnodes, G):
    cnode_neibors = set(G.neighbors(cnode))
    cnode_neibors = cnode_neibors - cnodes
    neibors = []
    for cneibor in cnode_neibors:
        neibors.append(((clique_id, cneibor), 1))
    return neibors
def add(x, y):
    return x + y
def process_neibor(cnodes, cneibor, G):
    miss_nodes = cnodes - set(G.neighbors(cneibor))
    neibors = [cneibor] * len(miss_nodes)
    return list(zip(miss_nodes, neibors))
def process_df(sc, df, sep, glom, G):
    rdd =  sc.parallelize(df.values)
    rdd = rdd.map(lambda x : (x[0], set(x[1].strip().split(sep))))
    rddmap = rdd.collectAsMap()
    rdd = rdd.flatMap(lambda x : cliques_tonode(x, sep))
    #[(ID, clique_node)]

    rdd = rdd.flatMap(lambda x : process_node(x[0], x[1], rddmap[x[0]], G))
    #[((ID, cneibor), 1)]

    cneibor_count = rdd.reduceByKey(add)
    cneibor_count = cneibor_count.map(lambda x : (x[0][0], x[0][1], len(rddmap[x[0][0]]) - x[1]))
    #[(ID, cneibor, miss_count)]
    cneibor_count = cneibor_count.filter(lambda x: x[2] <= glom)
    impute_edges = cneibor_count.flatMap(lambda x : process_neibor(rddmap[x[0]], x[1], G))
    return impute_edges.collect()


def paraclique3(sc, cliques, sep, graph, glom):
    cliques.reset_index(drop = False, inplace = True)
    impute_edges = process_df(sc, cliques, sep, glom, graph)
    return impute_edges


sc = SparkContext.getOrCreate()


#<<<<<<< HEAD
#samplesize = 500
#=======
samplesize = 500
#>>>>>>> 3725b4273b385dafefba5178d842e2b4747963bc
samplenum = 10
inputfile = smallclique
runtime = pd.DataFrame(columns = ["WithoutMR", "ForMR", "MR"])
cliquesdata = pd.read_csv(inputfile, header = None, names = ["cliques"])

for i in range(samplenum):
    cliques = cliquesdata.sample(samplesize)
    
    start = time.time()
    res1 = paraclique1(cliques, sep, graph, glom)
    end = time.time()
    #pd.DataFrame(res1).to_csv(inputfile.split('.')[0] + ".res1", index = False, header = False)
    WithoutMR = end - start
    print(len(res1))
    start = time.time()
    res2 = paraclique2(sc, cliques, sep, graph, glom)
    end = time.time()
    print(len(res2))
    #pd.DataFrame(res2).to_csv(inputfile.split('.')[0] + ".res2", index = False, header = False)
    ForMR = end - start
    start = time.time()
    res3 = paraclique3(sc, cliques, sep, graph, glom)
    end = time.time()
    print(len(res3))
    #pd.DataFrame(res3).to_csv(inputfile.split('.')[0] + ".res3", index = False, header = False)
    MR = end - start
    tmp = pd.DataFrame([[WithoutMR, ForMR, MR]], columns = ["WithoutMR", "ForMR", "MR"])
    runtime = pd.concat([runtime, tmp])
# runtime.to_csv(inputfile.split('.')[0] + str(samplesize) + "-" + str(samplenum) + ".time", index = False)
runtime.to_csv('mixed_' + str(samplesize) + "-" + str(samplenum) + ".time", index = False)

for output in fdownload.keys():
    os.system("rm " + output)

#print(len(res1))
#print(len(res2))
#print(len(res3))
