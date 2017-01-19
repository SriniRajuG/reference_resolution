from __future__ import division
from __future__ import print_function
import sys
import json
import networkx as nx
from io import open
from nltk.util import ngrams
import numpy as np
import re

import brand
import category

# BM - Brand Master - a graph (practically a tree)
# CM - Category Master - a graph
# MM - Measurement Master - hash table: keys are ids and values are list of compiled regex objects
# msmnt - measurement

# special_char_1 = ['!', "'", '"', '.', ',', '`', '@', '#', '$', '%', '*', '(', ')', '[', ']']


def load_config_file():
    try:
        config_fo = open('./rrconfig.json', 'r')
    except IOError as e:
        print(e)
        sys.exit()
    else:
        return json.load(config_fo)


def load_class_info_files(config_data):
    class_info_data = {}
    for seg_id, v in config_data.items():
        if 'class_info' in v:
            try:
                fo = open(v['class_info'], 'r')
            except IOError as e:
                print(e)
            else:
                class_info_data[seg_id] = json.load(fo)
    return class_info_data


def load_edit_dist_params(config_data):
    edit_dist_params = {}
    for seg_id, v in config_data.items():
        if 'params' in v:
            insert_costs = np.ones(128)
            delete_costs = np.ones(128)
            substitute_costs = np.ones((128, 128))
            ins_scores_map = v['params']['ins']
            del_scores_map = v['params']['del']
            subs_scores_map = v['params']['subs']
            if ins_scores_map:
                for key, value in ins_scores_map.items():
                    for char in key:
                        insert_costs[ord(char)] = value
            if del_scores_map:
                for key, value in del_scores_map.items():
                    for char in key:
                        delete_costs[ord(char)] = value
            if subs_scores_map:
                for key, value in subs_scores_map.items():
                    substitute_costs[ord(key[0]), ord(key[1])] = value
            edit_dist_params[seg_id] = {'threshold': v['params']['threshold'], 'ins': insert_costs,
                                        'del': delete_costs, 'subs': substitute_costs}
    return edit_dist_params


def load_master_files(config_data, attributes_data):
    special_chars = config_data['special_chars'].encode('ascii', errors='ignore')
    brand_type_id = attributes_data['references']['brand']['type_id']
    category_type_id = attributes_data['references']['category']['type_id']
    msmnt_type_id = attributes_data['references']['measurement']['type_id']
    BM_graph, brand_nodes, sub_brand_nodes = load_brand_master(config_data['masters'][brand_type_id], special_chars)
    CM_graph = load_category_master(config_data['masters'][category_type_id], special_chars)
    MM = load_msmnt_master(config_data['masters'][msmnt_type_id])
    return BM_graph, brand_nodes, sub_brand_nodes, CM_graph, MM


def load_brand_master(file_name, special_chars):
    lower_case_master = make_lower_case_name(file_name, special_chars)
    BM_root_node_map = {'Name': 'brand_master', 'id': 'brand_master'}
    BM_graph = master_to_graph(lower_case_master, BM_root_node_map)
    brand_nodes = BM_graph.successors(BM_root_node_map['id'])
    sub_brand_nodes = []
    for brand_node in brand_nodes:
        sub_brand_nodes.extend(BM_graph.successors(brand_node))
    return BM_graph, brand_nodes, sub_brand_nodes


def load_category_master(file_name, special_chars):
    lower_case_master = make_lower_case_name(file_name, special_chars)
    return master_to_graph(lower_case_master)


def load_msmnt_master(file_name):
    lower_case_master = make_lower_case_name(file_name)
    MM = compile_msmnt_units(lower_case_master)
    return MM


def compile_msmnt_units(msmnt_master_json):
    '''

    :param msmnt_master_json:
    :return msmnt_master: hash-table: keys are measurement ids and values are lists of compiled regex objects
    '''
    msmnt_master = dict()
    number_regex_str = r'\d+(?:\.\d+)?'
    space_regex_str = r'\s{0,}'
    for msmnt_map in msmnt_master_json:
        msmnt_regex_objs = list()
        msmnt_id = msmnt_map['id']
        msmnt_units = list()
        msmnt_units.append(msmnt_map['Name'])
        for unit in msmnt_map['Alias']:
            msmnt_units.append(unit)
        for unit in msmnt_units:
            if unit:
                if unit[-1] != 's':
                    regex_str = number_regex_str + space_regex_str + '(' + unit + r'[s]?' + ')'
                    regex_obj = re.compile(regex_str)
                    msmnt_regex_objs.append(regex_obj)
                else:
                    regex_str = number_regex_str + space_regex_str + '(' + unit + ')'
                    regex_obj = re.compile(regex_str)
                    msmnt_regex_objs.append(regex_obj)
                    regex_str = number_regex_str + space_regex_str + '(' + unit[: -1] + ')'
                    regex_obj = re.compile(regex_str)
                    msmnt_regex_objs.append(regex_obj)
        msmnt_master[msmnt_id] = msmnt_regex_objs
    return msmnt_master


def make_lower_case_name(file_name, special_chars=None):
    try:
        fo = open(file_name, 'r')
    except IOError as e:
        print(e)
        sys.exit()
    else:
        temp_master_json = json.load(fo)
        for node_map in temp_master_json:
            node_name = node_map.get('Name')
            name_aliases = node_map.get('Alias')
            if node_name:
                lower_case_name = node_name.strip().lower().encode('ascii', errors='ignore')
                if special_chars:
                    name_no_special_char = lower_case_name.translate(None, special_chars)
                    node_map['Name'] = name_no_special_char
                else:
                    node_map['Name'] = lower_case_name
            else:
                print('map with id' + ' ' + node_map['id'] + ' ' + 'doesnt have the key "Name"')
                sys.exit()
            if name_aliases:
                processed_aliases = list()
                for alias in name_aliases:
                    lower_case_alias = alias.strip().lower().encode('ascii', errors='ignore')
                    if special_chars:
                        alias_no_special_char = lower_case_alias.translate(None, special_chars)
                        processed_aliases.append(alias_no_special_char)
                        node_map['Alias'] = processed_aliases
                    else:
                        processed_aliases.append(lower_case_alias)
                        node_map['Alias'] = processed_aliases
        return temp_master_json


def load_unlabelled_SKUs(file_name):
    try:
        raw_data_inp_fo = open(file_name, 'r')
    except IOError as e:
        print(e)
        sys.exit()
    else:
        raw_data = json.load(raw_data_inp_fo)
        return raw_data


def get_attribute_ids(config_data):
    try:
        attribute_ids_fo = open(config_data['attribute_ids'], 'r')
    except IOError as e:
        print(e)
        sys.exit()
    else:
        attributes_data = json.load(attribute_ids_fo)
        for val in attributes_data['references'].values():
            val['regex'] = re.compile(val['regex'])
        return attributes_data


def make_attrib_ids_to_label_func_map(attributes_data, BM_graph, brand_nodes, sub_brand_nodes, CM_graph):
    brand_type_id = attributes_data['references']['brand']['type_id']
    category_type_id = attributes_data['references']['category']['type_id']
    id_to_label_func_map = {}
    id_to_label_func_map[brand_type_id] = {'name': brand.label_brand, 'param': [BM_graph, brand_nodes, sub_brand_nodes]}
    id_to_label_func_map[category_type_id] = {'name': category.label_category, 'param': [CM_graph]}
    return id_to_label_func_map


def get_leaf_nodes(DG):  # *DG* stands for di-graph
    leaf_nodes = []
    for node in DG.nodes_iter():
        if not DG.successors(node):  # if *node* doesn't have successors/children
            leaf_nodes.append(node)
    return leaf_nodes


def master_to_graph(processed_master, root_node_map=None):
    DG = nx.DiGraph()  # DG stands for directed-graph
    # this loop adds all the nodes to the di_graph
    for node_map in processed_master:
        DG.add_node(node_map['id'], node_map)
    if root_node_map:
        root_node_id = root_node_map['id']
        DG.add_node(root_node_id, root_node_map)
        # this loop adds edges between the root-node (node without predecessors) and its child-nodes
        for node_map in processed_master:
            if 'Parents' not in node_map:
                # root nodes in json file do not have the key 'parent'
                # root-nodes in json (ones without the key 'parent') are not the root-nodes of the graph/tree
                if 'Type' in node_map:
                    v = {'Type': node_map['Type']}
                    DG.add_edge(root_node_id, node_map['id'], v)
                else:
                    DG.add_edge(root_node_id, node_map['id'])
    # this loop adds edges between all the other nodes
    for node_map in processed_master:
        if 'Parents' in node_map:
            for parent_map in node_map['Parents']:
                if 'Type' in node_map:
                    v = {'Type': node_map['Type']}
                    DG.add_edge(parent_map['id'], node_map['id'], v)
                else:
                    DG.add_edge(parent_map['id'], node_map['id'])
    return DG


# def lower_case_name(json_data):
#     for node_map in json_data:
#         node_name = node_map.get('Name')
#         if node_name:  # ensures that each map has the key 'Name' whose value is not empty
#             node_map['Name'] = node_name.strip().lower().encode('ascii', errors='ignore')
#         else:
#             print('map with id' + ' ' + node_map['id'] + ' ' + 'doesnt have the key "Name"')
#             sys.exit()
#     return json_data


# def N_UNIT_TYPES():  # 1-tradable, 2-descriptive and 3-compound
#     return 3


# def load_UOM_mstr(file_name):  # uom stands for Units Of Measurement
#     temp_UOM_mstr = [{}, {}, {}]
#     # UOM_mstr is a tuple of maps
#     # UOM_mstr[0] is map of tradable units and their aliases
#     # UOM_mstr[0].keys() is a sequence of all the tradable units
#     # UOM_mstr[0]['some_unit'] is a sequence of aliases to 'some_unit'
#     # UOM_mstr[1] is map of descriptive units and their aliases
#     tradable_idx = 0
#     descriptive_idx = 1
#     with open(file_name, encoding='Windows-1252') as UOM_fo:
#         UOM_reader = csv.reader(UOM_fo)
#         temp_list = map(tuple, UOM_reader)
#         UOM_rows_tup = tuple(temp_list)
#         n_rows = len(UOM_rows_tup)
#         UOM_type_col_idx = 0
#         unit_col_idx = 1
#         unit_alias_col_idx = 2
#         for idx_row in range(1, n_rows):
#             # 1 instead of 0 because 0th row is the header
#             # 0th column decides whether the  unit is 'descriptive' or 'tradable' etc
#             unit_type = UOM_rows_tup[idx_row][UOM_type_col_idx].lower().strip()
#             if unit_type == 'tradable':
#                 # this row has tradable unit
#                 unit = UOM_rows_tup[idx_row][unit_col_idx].lower().strip()
#                 if unit:
#                     temp_alias_1 = UOM_rows_tup[idx_row][unit_alias_col_idx].lower().strip()
#                     temp_alias_2 = temp_alias_1.split(',')
#                     aliases = []
#                     for i in temp_alias_2:
#                         aliases.append(i.strip())
#                     temp_UOM_mstr[tradable_idx][unit] = aliases
#             elif unit_type == 'descriptive':
#                 # this row has descriptive unit
#                 unit = UOM_rows_tup[idx_row][unit_col_idx].lower().strip()
#                 if unit:
#                     temp_alias_1 = UOM_rows_tup[idx_row][unit_alias_col_idx].lower().strip()
#                     temp_alias_2 = temp_alias_1.split(',')
#                     aliases = []
#                     for i in temp_alias_2:
#                         aliases.append(i.strip())
#                     temp_UOM_mstr[descriptive_idx][unit] = aliases
#             else:
#                 # this row has compound unit
#                 pass
#         for i in range(N_UNIT_TYPES()):
#             for j in temp_UOM_mstr[i]:
#                 temp_UOM_mstr[i][j] = tuple(temp_UOM_mstr[i][j])
#         return tuple(temp_UOM_mstr)


def make_ngrams(tokens, max_n=7):
    """
    n-grams are created from a list of one word tokens. Each n-gram is a tuple. Each tuple is converted to a string
    :param tokens: list of strings from which n-grams need to be generated. These tokens are created by splitting a
    sentence at spaces
    :param max_n: integer. Maximum value of n in n-grams. 1 if unigram and 2 if bigram etc
    :return list_of_str:
    """
    ngrams_map = {}
    n_tokens = len(tokens)
    for i in range(1, max_n + 1):
        if i <= n_tokens:
            ngrams_go = ngrams(tokens, i)  # go is generator object
            list_of_str = []
            for tup in ngrams_go:
                temp_str = ''
                counter = 0
                for string in tup:
                    if counter == 0:
                        temp_str = temp_str + string
                    else:
                        temp_str = temp_str + ' ' + string
                    counter += 1
                list_of_str.append(temp_str)
            ngrams_map[i] = list_of_str
        else:
            ngrams_map[i] = []
    return ngrams_map


# def filter_sequence(seq, itr):
#     '''
#     Remove elements of *itr* from *seq* and return a set
#     '''
#     for i in itr:
#         while i in seq:
#             seq.remove(i)
#     return set(seq)
