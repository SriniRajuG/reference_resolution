from __future__ import division

import brand

def label_category(sku_id_ngrams_map, sku_edit_dist_params, CM_graph):
    sku_id, ngrams_map = sku_id_ngrams_map
    category_ids = list()
    unlabelled_tokens = set()
    nodes_ed_map = {}  # node_ids are keys and list of min-edit-dists are values
    PD_tokens = set(ngrams_map[1])
    for node_id in CM_graph.nodes_iter():
        node_data = CM_graph.node[node_id]
        temp_name = node_data['Name']
        all_names = get_list_of_aliases(temp_name)
        min_ed_ratios_all_names = []
        min_ed_ratios = []  # minimum from *min_ed_ratios_all_names*
        for alias in all_names:
            n_words_in_node_name = len(alias.split())
            ngrams = list()
            for i in range(n_words_in_node_name - 1, n_words_in_node_name + 2):
                temp_ngrams = ngrams_map.get(i)
                if temp_ngrams:
                    ngrams.extend(temp_ngrams)
            # ngrams is a list of stings and not a list of tuples
            if ngrams:
                min_ed_ratios = brand.get_min_edit_dist_ratios(ngrams, alias, sku_edit_dist_params)
                min_ed_ratios_all_names.extend(min_ed_ratios)
        if min_ed_ratios_all_names:
            counter = 0
            for ed_ratio, token in min_ed_ratios_all_names:
                if not counter:
                    min_ed_ratio = ed_ratio
                else:
                    if ed_ratio < min_ed_ratio:
                        min_ed_ratio = ed_ratio
                counter += 1
            for ed_ratio, token in min_ed_ratios_all_names:
                if ed_ratio == min_ed_ratio:
                    min_ed_ratios.append((ed_ratio, token))
            nodes_ed_map[node_id] = min_ed_ratios
    matched_nodes, labelled_phrases = brand.get_ed_match(nodes_ed_map, sku_edit_dist_params)
    if matched_nodes:
        for match in matched_nodes:
            # match[0] is node_id and match[1] is min_ed_ratio
            conf_score = 1 - match[1]
            category_ids.append((match[0], conf_score))
        labelled_tokens = brand.split_labelled_phrases(labelled_phrases)
        unlabelled_tokens = brand.get_unlabelled_tokens(labelled_tokens, PD_tokens)
    return sku_id, category_ids, unlabelled_tokens


def get_list_of_aliases(phrase):
    # str is a string with backslashes
    # backslash is used to separate aliases of a phrase
    temp_aliases_list = phrase.split('/')
    aliases = []
    for i in temp_aliases_list:
        if i:  # only if i is not an empty string
            aliases.append(i.strip())
    return aliases
