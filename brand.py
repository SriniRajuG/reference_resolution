from __future__ import print_function
from __future__ import division
import re
from weighted_levenshtein import lev

import inp_files as inp


def label_brand(sku_id_ngrams_map, sku_edit_dist_params, BM_graph, brand_nodes, sub_brand_nodes):
    sku_id, ngrams_map = sku_id_ngrams_map
    PD_tokens = set(ngrams_map[1])
    unlabelled_tokens = set()
    brand_ids = {'solutions': list()}
    phrases = {'labelled': list()}
    check = {'found_leaf_node': False, 'found_brand': False, 'tried_sub_brand': False}

    def find_ed_node(di_graph, root_nodes, n_grams_map, sku_ed_params):
        node_ed_map = {}
        for node_id in root_nodes:
            node_name = di_graph.node[node_id]['Name']
            n_words_in_node_name = len(node_name.split())
            ngrams = []
            for i in range(n_words_in_node_name - 1, n_words_in_node_name + 2):
                temp_ngrams = n_grams_map.get(i)
                if temp_ngrams:
                    ngrams.extend(temp_ngrams)
            # ngrams is a list of stings and not a list of tuples
            if ngrams:
                min_ed_ratios = get_min_edit_dist_ratios(ngrams, node_name, sku_ed_params)
                node_ed_map[node_id] = min_ed_ratios
        matched_nodes, labelled_phrases = get_ed_match(node_ed_map, sku_ed_params)
        if matched_nodes:
            check['found_brand'] = True
            for match in matched_nodes:
                # match[0] is node_id and match[1] is min_ed_ratio
                if not match[1]: # if exact match.
                    child_nodes = di_graph.successors(match[0])
                    # if *child_nodes* is empty, *node_id* is a node with zero successors (leaf_node)
                    if not child_nodes:
                        conf_score = 1.0 - match[1]
                        brand_ids['solutions'] += [(match[0], conf_score)]
                        check['found_leaf_node'] = True
                        phrases['labelled'].extend(labelled_phrases)
                    else:
                        find_ed_node(di_graph, child_nodes, n_grams_map, sku_ed_params)
                    if not check['found_leaf_node']:
                        conf_score = 1.0 - match[1]
                        brand_ids['solutions'] += [(match[0], conf_score)]
                        check['found_leaf_node'] = True
                        phrases['labelled'].extend(labelled_phrases)
                else:
                    conf_score = 1.0 - match[1]
                    brand_ids['solutions'] += [(match[0], conf_score)]
                    check['found_leaf_node'] = True
                    phrases['labelled'].extend(labelled_phrases)
                check['found_leaf_node'] = False
        else:
            if not check['found_brand'] and (not check['tried_sub_brand']):
                check['tried_sub_brand'] = True
                # try with sub-brands
                find_ed_node(di_graph, sub_brand_nodes, n_grams_map, sku_ed_params)

    find_ed_node(BM_graph, brand_nodes, ngrams_map, sku_edit_dist_params)
    labelled_tokens = split_labelled_phrases(set(phrases['labelled']))
    unlabelled_tokens = get_unlabelled_tokens(labelled_tokens, PD_tokens)
    return sku_id, brand_ids['solutions'], unlabelled_tokens


# def n_words_in_string(inp_str, process=True):
#     if process:
#         temp_inp_str = inp_str
#         for char in inp.special_char_1:
#             if char in temp_inp_str:
#                 stripped_tokens = [token.strip() for token in temp_inp_str.split(char)]
#                 temp_inp_str = char.join(stripped_tokens)
#         return len(temp_inp_str.split())
#     else:
#         return len(inp_str.split())


def get_ed_match(nodes_ed_map, sku_edit_dist_params):
    '''

    :param nodes_ed_map: hash-table. Node-ids are keys and values are [(ed_0, phrase_0), ..., (ed_n, phrase_n)]
    :param sku_edit_dist_params: hash-table.
    :return matched_nodes: list of two-element tuples. First elem. is node-id and second elem. is edit distance
    :return labelled_phrases: set of labelled phrases
    '''
    matched_nodes = list()
    labelled_phrases = set()
    for node_id, ed_phrase_list in nodes_ed_map.items():
        is_node_selected = False
        selected_edit_dists = list()
        for edit_dist, phrase in ed_phrase_list:
            if edit_dist <= sku_edit_dist_params['threshold']:
                is_node_selected = True
                selected_edit_dists.append(edit_dist)
                labelled_phrases.add(phrase)
        if is_node_selected and selected_edit_dists:
            matched_nodes.append((node_id, min(selected_edit_dists)))
    return matched_nodes, labelled_phrases


def get_min_edit_dist_ratios(phrases, node_name, sku_edit_dist_params):
    '''

    :param phrases: list of strings
    :param node_name: string
    :param sku_edit_dist_params: hash-table
    :return: min_ed_ratios: list of two-element tuples. First elem is edit-dist-ratio. Second elem is
    corresponding phrase from *phrases*
    '''
    ed_ratio_phrases = list()
    min_ed_ratios = list()
    n_char_node_name = len(node_name)
    for phrase in phrases:
        ed = lev(phrase, node_name, insert_costs=sku_edit_dist_params['ins'],
                 delete_costs=sku_edit_dist_params['del'],
                 substitute_costs=sku_edit_dist_params['subs'])
        ed_ratio = ed/n_char_node_name
        ed_ratio_phrases.append((ed_ratio, phrase))
    counter = 0
    for ed_ratio, phrase in ed_ratio_phrases:
        if not counter:
            min_ed_ratio = ed_ratio
        else:
            if ed_ratio < min_ed_ratio:
                min_ed_ratio = ed_ratio
        counter += 1
    for ed_ratio, phrase in ed_ratio_phrases:
        if ed_ratio == min_ed_ratio:
            min_ed_ratios.append((ed_ratio, phrase))
    return min_ed_ratios


def string_search(string1, string2):  # search for string1 in word-boundaries of string2
    if re.search(r"\b" + re.escape(string1) + r"\b", string2):
        return string1


def split_labelled_phrases(labelled_phrases):
    labelled_tokens = set()
    for i in labelled_phrases:
        for j in i.split():
            labelled_tokens.add(j)
    return labelled_tokens


def get_unlabelled_tokens(labelled_tokens, PD_tokens):
    for i in labelled_tokens:
        PD_tokens.remove(i)
    return PD_tokens