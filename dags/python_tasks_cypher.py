import requests
import json
import random
import pandas as pd
import re
import numpy as np
from py2neo import Graph, Node, NodeMatcher, Relationship
#from py2neo import *
#import pickle5 as pickle


def node_mapper():

    #memes = pd.read_pickle("/opt/airflow/dags/memes.pkl")
    with open('/opt/airflow/dags/kym.csv') as data:
        memes = pd.read_csv(data)

    try:
        graph = Graph("bolt://neo:7687")
        print("YES YES YES!!!")
        graph.delete_all()

        ## Mapping nodes
        for i in range(memes.shape[0]):
                
                id_ = memes.iloc[i,1]
                title_ = memes.iloc[i,0]
                        
                ## Tags array
                if str(memes.iloc[i,4]) == 'nan':
                    tags_ = []
                elif "," in memes.iloc[i,4]:
                    tags_ = memes.iloc[i,4].split(",")
                else:
                    n_list = []
                    n_list.append(memes.iloc[i,4])
                    tags_ = n_list
                
                ## Keywords array
                if str(memes.iloc[i,5]) == 'nan':
                    search_keywords_ = []
                elif "," in memes.iloc[i,5]:
                    search_keywords_ = memes.iloc[i,5].split(",")
                else:
                    n_list = []
                    n_list.append(memes.iloc[i,5])
                    search_keywords_ = n_list
                    
                
                height_ = int(memes.iloc[i,9])
                width_ = int(memes.iloc[i,10])
                origin_ = memes.iloc[i,11]
                status_ = memes.iloc[i,12]
                year_ = int(memes.iloc[i,13])
                
                node = Node("Meme", 
                                id=id_,
                                title = title_,
                                tags = tags_,
                                search_keywords = search_keywords_,
                                height = height_,
                                width = width_,
                                origin = origin_,
                                status = status_,
                                year = year_
                                )
                
                graph.create(node)

    except:
        print("Error Connection to Neo4j DB!!")


def edge_mapper():

    #memes = pd.read_pickle("/opt/airflow/dags/memes.pkl")
    with open('/opt/airflow/dags/kym.csv') as data:
        memes = pd.read_csv(data)

    try:
        graph = Graph("bolt://neo:7687")
        matcher = NodeMatcher(graph)
        print("YES YES YES!!!")
        ## Mapping edges
        for i in range(memes.shape[0]):
                
                id_ = memes.iloc[i,1]
                
                ## Children
                if str(memes.iloc[i,8]) == 'nan':
                    children_ = []
                elif "," in memes.iloc[i,8]:
                    children_ = memes.iloc[i,8].split(",")
                else:
                    n_list = []
                    n_list.append(memes.iloc[i,8])
                    children_ = n_list
                    
                if len(children_) > 0:
                    for cn in children_:
                        
                        targets = memes[[cn in x for x in memes['url']]] 
                        
                        ## Add direct edges
                        if(len(memes.loc[memes["url"] == cn, "title"]) != 0):
                            
                            
                            property_dictionary = {'edge_creation': "direct"}
                            
                            ## Add source
                            source_node =  matcher.match('Meme', id=id_).first()
                            
                            ## Add target
                            target_key = memes.loc[memes["url"] == cn, "url"].item()
                            target_node =  matcher.match('Meme', id=target_key).first()
                            
                            ## Add relationship
                            graph.create(Relationship(source_node, "IS_PARENT_TO", target_node, **property_dictionary))
                            
                        ## Infer parent based on string similarity
                        elif (len(targets) != 0):
                            
                            property_dictionary = {'edge_creation': "inferred"}
                            
                            ## Add source
                            source_node =  matcher.match('Meme', id=id_).first()
                            
                            ## Add target
                            
                            for t in range(len(targets)):
                                
                                target_key = targets.iloc[t,1]
                                target_node =  matcher.match('Meme', id=target_key).first()
                            
                                ## Add relationship
                                graph.create(Relationship(source_node, "IS_PARENT_TO", target_node))


                                ## Add relationship
                                graph.create(Relationship(source_node, "IS_PARENT_TO", target_node, **property_dictionary))

    except:
        print("Error Connection to Neo4j DB!!")


def run_analysis():

    #memes = pd.read_pickle("/opt/airflow/dags/memes.pkl")
    #memes = pd.read_csv('/opt/airflow/dags/kym.csv')

    try:
        graph = Graph("bolt://neo:7687")
        print("YES YES YES!!!")

        ## Running analysis
        analysis_summary = pd.DataFrame(columns=['Question', 'Result'], index=range(0))
            
        ## How many edges do we have in total?
        query = "MATCH (t:Meme) -[:IS_PARENT_TO]-> (s:Meme) RETURN *"
        result = graph.run(query).data()
        
        Question_ = "Edges in total"
        Result_ = str(len(result))
        analysis_summary = analysis_summary.append({'Question': Question_, 'Result': Result_}, ignore_index=True)
        
        
        ## How many edges do we have in total that are directly obtained from data?
        query = """
        MATCH (m:Meme)-[r:IS_PARENT_TO]->(t:Meme) 
        WHERE r.edge_creation = 'direct'
        RETURN *
        """
        result = graph.run(query).data()
        
        Question_ = "... out of which are directly obtained from data"
        Result_ = str(len(result))
        analysis_summary = analysis_summary.append({'Question': Question_, 'Result': Result_}, ignore_index=True)
        
        
        ## How many edges do we have in total that are inferred?
        query = """
        MATCH (m:Meme)-[r:IS_PARENT_TO]->(t:Meme) 
        WHERE r.edge_creation = 'inferred'
        RETURN *
        """
        result = graph.run(query).data()
        
        Question_ = "... out of which are inferred based on url similarity"
        Result_ = str(len(result))
        analysis_summary = analysis_summary.append({'Question': Question_, 'Result': Result_}, ignore_index=True)
        
        
        ## How many nodes do we have in total?
        query = "MATCH (m:Meme) RETURN *"
        result = graph.run(query).data()
        
        Question_ = "Nodes in total"
        Result_ = str(len(result))
        analysis_summary = analysis_summary.append({'Question': Question_, 'Result': Result_}, ignore_index=True)
        
        
        ### What are the most popular meme origin communities, how many memes have they as record in our DB
        query = """
        MATCH (m:Meme)
        WITH m.origin AS origin_community, count(*) AS count
        ORDER BY count DESC
        WITH origin_community, COLLECT(count) AS counts
        RETURN origin_community, counts[0]
        """
        result_ = graph.run(query).data()
        
        Question_ = "Most popular meme communities and related meme counts (TOP5)"
        Result_ = str(result_[0:5])
        analysis_summary = analysis_summary.append({'Question': Question_, 'Result': Result_}, ignore_index=True)
        

        ## Find children of given node - one hop children
        query = """
        MATCH (m:Meme {title:"Senator Armstrong"})-[*1]->(t:Meme) 
        RETURN m.title,t.title
        """
        result = graph.run(query).data()
        
        Question_ = "One hop neighbours for meme titled 'Senator Armstrong'"
        Result_ = str(result)
        analysis_summary = analysis_summary.append({'Question': Question_, 'Result': Result_}, ignore_index=True)
        
        
        ## Find children of given node - indirect children
        query = """
        MATCH (m:Meme {title:"Senator Armstrong"})-[*2]->(t:Meme) 
        RETURN m.title,t.title
        """
        result = graph.run(query).data()
        
        Question_ = "Indirectly connected memes meme titled 'Senator Armstrong'"
        Result_ = str(result)
        analysis_summary = analysis_summary.append({'Question': Question_, 'Result': Result_}, ignore_index=True)
        
        
        ## How many Connections there are which are not direct and rund via another node?
        query = """
        MATCH (m:Meme)-[*2]->(t:Meme) 
        RETURN m.title,t.title
        """
        result = graph.run(query).data()
        
        Question_ = "Indirect connections in total"
        Result_ = str(len(result))
        analysis_summary = analysis_summary.append({'Question': Question_, 'Result': Result_}, ignore_index=True)
        
        
        ## Output list of indirect connections ... a simple data dump
        with open('indirect_connections.txt', 'w') as f:
            for item in Result_:
                f.write("%s\n" % item)
            
        
        ## Write results to a cvs file
        analysis_summary.to_csv('/opt/airflow/dags/analysis_summary.csv', index=False)

    except:
        print("Error Connection to Neo4j DB!!")


 

    
    

    