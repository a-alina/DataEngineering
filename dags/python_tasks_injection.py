from datetime import date
import pandas as pd
import numpy as np



def insert():
    memes = pd.read_csv('/opt/airflow/dags/kym.csv')
    memes['tags'] = memes['tags'].apply(lambda row: row.split(',') if type(row) == str else row)

    # unique tags 
    collect = []
    for row in memes['tags']:
        if type(row) == list:
            for tag in row:
                if tag != '':
                    collect.append(tag)
        else: pass

    tags_encoding = {k: v for (k,v) in zip(set(collect), [i for i in range(len(set(collect)))])}
    tags_df = pd.DataFrame.from_dict({'tag_id': list(tags_encoding.values()), 'tag_name':list(tags_encoding.keys())})

    with open("/opt/airflow/dags/tags.sql", "a") as f:
        df_iterable = tags_df.iterrows() 
        for index, row in df_iterable:
            tag_id = row['tag_id']
            tag_name = row['tag_name']

            f.write(
                "INSERT INTO tags VALUES ("
                f"{tag_id}', '{tag_name}'"
                ");\n"
                    )

    f.close()


def tag_create():
    with open("/opt/airflow/dags/tags.sql", "w") as f:
        f.write(
        "CREATE TABLE IF NOT EXISTS tags (\n"
        "tag_id INT,\n"
        "tag_name VARCHAR(255));\n"
    )

def main_table():
    memes = pd.read_csv('/opt/airflow/dags/kym.csv')
    memes = memes.reset_index()
    memes.rename(columns={'index':'meme_id'}, inplace=True)
    meme_fact = memes[['meme_id', 'title', 'added', 'year']]
    meme_fact['url_id'] = meme_fact['meme_id']

    meme_fact['image_meta_id'] = 0

    tag_comb_id = memes.apply(lambda row: row['meme_id'] if (type(row['tags']) == str or type(row['search_keywords']) == str) else None, axis=1)
    tag_comb_id.name = 'tag_id'
    meme_fact = pd.concat([meme_fact, tag_comb_id], axis=1)

    ref_comb_final = memes.apply(lambda row: row['meme_id'] if (type(row['origin']) == str or type(row['ref_site']) == str) else None, axis=1)
    ref_comb_final.name = 'reference_id'
    meme_fact = pd.concat([meme_fact, ref_comb_final], axis=1) 

    rel_id = memes.apply(lambda row: row['meme_id'] if (type(row['children']) == str or type(row['parent']) == str or type(row['siblings'])==str) else None, axis=1)
    rel_id.name = 'relationship_id'
    meme_fact = pd.concat([meme_fact, rel_id], axis=1)
    
    meme_fact.fillna(value='NULL', inplace=True)

    with open("/opt/airflow/dags/main.sql", "w") as f:
        f.write(
            "CREATE TABLE IF NOT EXISTS main_fact (\n"
            "Meme_ID INT,\n"
            "Ttile_Name VARCHAR(255),\n"
            "Date_added DATE,\n"
            "Year_Of_Spread INT,\n"
            "Image_Meta INT,\n"
            "URL_ID INT,\n"
            "Tag_ID INT,\n"
            "Reference_ID INT,\n"
            "Relationship_ID INT);\n"

    )

        df_iterable = meme_fact.iterrows() 
        for index, row in df_iterable:
            meme_id = row['meme_id']
            title = row['title']
            added = row['added']
            year = row['year']
            url_id = row['url_id']
            image_meta_id= row['image_meta_id']
            tag_id = null(row['tag_id'])
            reference_id = null(row['reference_id'])
            relationship_id = null(row['relationship_id'])

            f.write(
                "INSERT INTO main_fact VALUES ("
                f"'{meme_id}', '{title}', '{added}', '{year}', {image_meta_id}, {url_id}, {tag_id}, {reference_id}, {relationship_id}"
                ");\n"
                    )
        f.close()

def null(row):
    if row == 'NULL':
        return 'NULL'
    else:
        return int(row)

def rel_id_extract(row, memes):
  """extracting related meme ids if they are in the dataset """

  if type(row) == str:
    l = []
    row = row.split(',')
    for i in row:
      try:
        memes[memes['url'] == i].index[0]
      except IndexError:
        pass
      else:
        l.append(memes[memes['url'] == i].index[0])
    return l if len(l) != 0 else np.nan
  else:
    row

def rel():
    memes = pd.read_csv('/opt/airflow/dags/kym.csv')
    children_id = memes['children'].apply(lambda row: rel_id_extract(row, memes))
    children_id = children_id[~children_id.isna()].explode().reset_index()
    children_id['type_of_relationship'] = 1
    children_id.rename(columns={'index':'original_meme_id', 'children':'relationship_id'}, inplace=True)

    parent_id = memes['parent'].apply(lambda row: rel_id_extract(row, memes))

    parent_id = parent_id[~parent_id.isna()].explode().reset_index()
    parent_id['type_of_relationship'] = 2
    parent_id.rename(columns={'index':'original_meme_id', 'parent':'relationship_id'}, inplace=True)

    siblings_id = memes['siblings'].apply(lambda row: rel_id_extract(row, memes))

    siblings_id = siblings_id[~siblings_id.isna()].explode().reset_index()
    siblings_id['type_of_relationship'] = 3
    siblings_id.rename(columns={'index':'original_meme_id', 'siblings':'relationship_id'}, inplace=True)
    siblings_id = siblings_id[siblings_id['original_meme_id'] != siblings_id['relationship_id']]

    relationship_df = pd.concat([children_id, parent_id, siblings_id])
    relationship_df = relationship_df.reset_index()
    relationship_df.rename(columns={'index':'relationship_unique_id'}, inplace=True)

    with open("/opt/airflow/dags/rel.sql", "w") as f:
        f.write(
            "CREATE TABLE IF NOT EXISTS relationship_table (\n"
            "Relationship_Unique_ID INT,\n"
            "Original_Meme_ID INT,\n"
            "Relationship_ID INT,\n"
            "Type_Of_Relationship INT);\n"

    )

        df_iterable = relationship_df.iterrows() 
        for index, row in df_iterable:
            original_id = row['original_meme_id']
            relationship_id = row['relationship_id']
            type = row['type_of_relationship']
            rel_unique = row['relationship_unique_id']

            f.write(
                "INSERT INTO relationship_table VALUES ("
                f"{rel_unique}, {original_id}, {relationship_id}, {type}"
                ");\n"
                    )
        f.close()

def url():
    memes = pd.read_csv('/opt/airflow/dags/kym.csv')
    url_fact_template = memes[['url', 'template_image_url', 'status']].reset_index()
    url_fact_template.rename(columns={'index': 'url_id'}, inplace=True)

    with open("/opt/airflow/dags/url.sql", "w") as f:
        f.write(
            "CREATE TABLE IF NOT EXISTS url_table (\n"
            "URL_ID INT,\n"
            "URL VARCHAR(255),\n"
            "Template_Image_URL VARCHAR(255),\n"
            "Status  VARCHAR(255));\n"

    )

        df_iterable = url_fact_template.iterrows() 
        for index, row in df_iterable:
            url_id = row['url_id']
            url = row['url']
            if "'" in url:
                url = url.replace("'", "")
            template = row['template_image_url']
            if "'" in template:
                template = template.replace("'", "")
            status = row['status']

            f.write(
                "INSERT INTO url_table VALUES ("
                f"{url_id}, '{url}', '{template}', '{status}'"
                ");\n"
                    )
        f.close()



def reference():
    memes = pd.read_csv('/opt/airflow/dags/kym.csv')

    ref_id_df = memes['ref_site'].apply(lambda row: [i.strip() for i in row.split(',')] if type(row) == str else row)

    collect_ref = []
    for row in ref_id_df:
        if type(row) == list:
            for ref in row:
                collect_ref.append(ref)
        else:
            pass

    ref_id_encoding = {k: v for (k,v) in zip(set(collect_ref), [i for i in range(len(set(collect_ref)))])}

    ref = pd.DataFrame.from_dict({'ref_id': list(ref_id_encoding .values()), 'ref_name':list(ref_id_encoding .keys())})


    origin_id_encoding = {k: v for (k,v) in zip(memes['origin'].unique(), 
                [i for i in range(len(set(collect_ref)), len(set(collect_ref)) + len(memes['origin'].unique()))])}
    ori = pd.DataFrame.from_dict({'ref_id': list(origin_id_encoding .values()), 'ref_name':list(origin_id_encoding.keys())})
    ori['type_of_origin'] = 1
    ref['type_of_origin'] = 2
    reference_sites = pd.concat([ref, ori])
    reference_sites = reference_sites[~reference_sites['ref_name'].isna()]

    with open("/opt/airflow/dags/reference_sites.sql", "w") as f:
        f.write(
            "CREATE TABLE IF NOT EXISTS reference_sites (\n"
            "Reference_ID INT,\n"
            "Reference_Name VARCHAR(255),\n"
            "Type_Of_Origin_ID INT);\n"

    )

        df_iterable = reference_sites.iterrows() 
        for index, row in df_iterable:
            name = row['ref_name']
            if "'" in name:
                name = name.replace("'", "")
            id = row['ref_id']
            type_ = row['type_of_origin']

            f.write(
                "INSERT INTO reference_sites VALUES ("
                f"{id}, '{name}', {type_}"
                ");\n"
                    )
        f.close()
        


     #### reference_combinations

    ref_comb = ref_id_df.apply(lambda x: [ref_id_encoding[i] for i in x] \
                                         if type(x) == list else x)

    origin_comb = memes['origin'].apply(lambda x: origin_id_encoding[x] \
                                         if type(x) == str else x)

    ref_comb_ = ref_comb[~ref_comb.isna()].explode().reset_index()
    ref_comb_.rename(columns={'index':'ref_comb_id', 'ref_site': 'ref_id'}, inplace=True)

    origin_comb = origin_comb[~origin_comb.isna()].astype(int).reset_index()
    origin_comb.rename(columns={'index':'ref_comb_id', 'origin': 'ref_id'}, inplace=True)
    ref_comb_final = pd.concat([origin_comb, ref_comb_])
    
    with open("/opt/airflow/dags/reference_combinations.sql", "w") as f:
        f.write(
            "CREATE TABLE IF NOT EXISTS reference_combinations (\n"
            "Reference_Combination_ID INT,\n"
            "Reference_ID INT);\n"

    )

        df_iterable = ref_comb_final.iterrows() 
        for index, row in df_iterable:
            comb_id = row['ref_comb_id']
            id = row['ref_id']


            f.write(
                "INSERT INTO reference_combinations VALUES ("
                f"{comb_id}, {id}"
                ");\n"
                    )
        f.close()

def image_data():
    memes = pd.read_csv('/opt/airflow/dags/kym.csv')
    image_data = memes[['width', 'height']]
    image_data = image_data.drop_duplicates()
    

    with open("/opt/airflow/dags/image_data.sql", "w") as f:
        f.write(
            "CREATE TABLE IF NOT EXISTS image_data (\n"
            "ID INT,\n"
            "Width INT,\n"
            "Height_ID INT);\n"

    )

        df_iterable = image_data.iterrows() 
        for index, row in df_iterable:
            id = 0
            width = row['width']
            height = row['height']


            f.write(
                "INSERT INTO image_data VALUES ("
                f"{id},{width}, {height}"
                ");\n"
                    )
        f.close()

def tags():
    memes = pd.read_csv('/opt/airflow/dags/kym.csv')
    memes['tags'] = memes['tags'].apply(lambda row: row.split(',') if type(row) == str else row)

    # unique tags 
    collect = []
    for row in memes['tags']:
        if type(row) == list:
            for tag in row:
                if tag != '':
                    collect.append(tag)
    else: pass

    tags_encoding = {k: v for (k,v) in zip(set(collect), [i for i in range(len(set(collect)))])}    
    tags_df = pd.DataFrame.from_dict({'tag_id': list(tags_encoding.values()), 'tag_name':list(tags_encoding.keys())})


    search_keywords_df = memes['search_keywords'].apply(lambda row: [i.strip() for i in row.split(',')] if type(row) == str else row)
    collect_s = []
    for row in search_keywords_df:
        if type(row) == list:
            for keyword in row:
                collect_s.append(keyword)
        else:
            pass
    
    keywords_encoding = {k: v for (k,v) in zip(set(collect_s), [i for i in range(len(set(collect_s)))])}
    keywords_df = pd.DataFrame.from_dict({'tag_id': list(keywords_encoding.values()), 'tag_name':list(keywords_encoding.keys())})

    tags_df['type_id'] = 1 
    keywords_df['type_id'] = 2

    tags_df_final = pd.concat([tags_df, keywords_df])

    with open("/opt/airflow/dags/tags.sql", "w") as f:
        f.write(
            "CREATE TABLE IF NOT EXISTS tags (\n"
            "Tag_ID INT,\n"
            "Tag_Name VARCHAR(255),\n"
            "Type_ID INT);\n"

    )

        df_iterable = tags_df_final.iterrows() 
        for index, row in df_iterable:
            tag_id = row['tag_id']
            tag_name = row['tag_name']
            type_id = row['type_id']


            f.write(
                "INSERT INTO tags VALUES ("
                f"{tag_id}, '{tag_name}',{type_id} "
                ");\n"
                    )
        f.close()


    ### tag_combinations

    tag_combination_df = memes['tags'].apply(lambda x: [tags_encoding[i] for i in x if i != ''] \
                                         if type(x) == list else x)
    keywords_combination_df = search_keywords_df.apply(lambda x: [keywords_encoding[i] for i in x] \
                                         if type(x) == list else x)
    exp_s = keywords_combination_df[~keywords_combination_df.isna()].explode().reset_index()
    exp_s.rename(columns={'index':'tag_comb_id', 'search_keywords': 'tag_id'}, inplace=True)
    exp_t = tag_combination_df[~tag_combination_df.isna()].explode().reset_index()
    exp_t.rename(columns={'index':'tag_comb_id', 'tags': 'tag_id'}, inplace=True)
    tag_comb = pd.concat([exp_t, exp_s])

    with open("/opt/airflow/dags/tags_combinations.sql", "w") as f:
        f.write(
            "CREATE TABLE IF NOT EXISTS tags_combinations (\n"
            "Tag_Comb_ID INT,\n"
            "Type_ID INT);\n"

    )

        df_iterable =  tag_comb.iterrows() 
        for index, row in df_iterable:
            tag_comb_id = row['tag_comb_id']
            tag_id_c = row['tag_id']


            f.write(
                "INSERT INTO tags_combinations VALUES ("
                f"{tag_comb_id}, {tag_id_c} "
                ");\n"
                    )
        f.close()
