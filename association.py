'''
Calculate the association between product-type and brand
Input: Labelled data. For a given product, product-name and product-type are labelled either manually or using a code.
'''

# The presence of brand, subbrand or variant is a function of the product-segment. For eg. brand-master for FMCG segment
# includes a hirarchical brand, subbrand and variant. Other segments need not have this structure. Irrespective of
# segment, every product will have a product-name (sometimes referred to as brand) and a product-type (sometimes also
# referred to as product category)


from __future__ import division
from __future__ import print_function
from collections import defaultdict, Counter
import json
import sys
import re

import inp_files as inp

def make_attrib_id_regex_map():
    global attrib_id_po_map
    attrib_id_po_map = {}
    # po stands for the pattern_object obtained after compiling a regular expression
    for v in inp.attributes_data['references'].values():
        attrib_id_po_map[v['type_id']] = re.compile(v['regex'])


def get_attribute_association(attribute_A_id, attribute_B_id):
    brand_type_id = inp.attributes_data['references']['brand']['type_id']
    category_type_id = inp.attributes_data['references']['category']['type_id']
    for attrib_name, v in inp.attributes_data['references'].items():
        if attribute_A_id == v["type_id"]:
            attrib_A_name = attrib_name
        elif attribute_B_id == v["type_id"]:
            attrib_B_name = attrib_name
    AtoB_output_file_name = attrib_A_name + '_' + attrib_B_name + '_' + 'assoc.json'
    BtoA_output_file_name = attrib_B_name + '_' + attrib_A_name + '_' + 'assoc.json'
    labelled_skus_file_name = inp.config_data['labelled_SKUs']
    try:
        AtoB_output_fo = open(AtoB_output_file_name, 'w')
        BtoA_output_fo = open(BtoA_output_file_name, 'w')
        labelled_skus_fo = open(labelled_skus_file_name, 'r')
    except IOError as e:
        print(e)
        sys.exit()
    else:
        labelled_skus_data = json.load(labelled_skus_fo)
        # LDS_data is a list of maps with the following structure
        # {
        #   "id_SKU": "id_0",
        #   "attribute_A_id":[["label_A_id", ["label_A_value"]]],
        #   "attribute_B_id":[["label_B_id", ["label_B_value"]]],
        #    ...
        # }
        # each item in LDS is an SKU(stock keeping unit)/product
        attributeA_to_attributeB = []
        make_attrib_id_regex_map()
        for map in labelled_skus_data:
            if 'segment' in map:
                segment_ids = map['segment']
                if segment_ids: # segment_ids is not empty
                    segment_id = map['segment'][0] # <----- currently taking only one segment.Should take a list
                    for temp_atrb_id, v in inp.class_info_data[segment_id]['references'].items():
                        # v[0] is attribute-name
                        global attrib_id_po_map
                        if v[1] == attribute_A_id and attrib_id_po_map[attribute_A_id].search(v[0]):
                            attrib_A_id = temp_atrb_id
                        elif v[1] == attribute_B_id and attrib_id_po_map[attribute_B_id].search(v[0]):
                            attrib_B_id = temp_atrb_id
                    labels_A = map.get(attrib_A_id)
                    labels_B = map.get(attrib_B_id)
                    if labels_A and labels_B:  # ensures both labels_A and labels_B are not null
                        # if attribute_A_id in [inp.brand_type_id, inp.category_type_id]:
                        if attribute_A_id in [brand_type_id, category_type_id]:
                            attrib_A_label_ids = []
                            for label in labels_A:
                                global all_parent_ids
                                all_parent_ids = []
                                leaf_node_id = label[0]  # label[1] is the string-value
                                get_parent_ids(leaf_node_id, attribute_A_id, brand_type_id, category_type_id)
                                attrib_A_label_ids.append(all_parent_ids)
                        else:
                            attrib_A_label_ids = []
                            for label in labels_A:
                                attrib_A_label_ids.append(label[0])
                        if attribute_B_id in [brand_type_id, category_type_id]:
                            attrib_B_label_ids = []
                            for label in labels_B:
                                global all_parent_ids
                                all_parent_ids = []
                                leaf_node_id = label[0]
                                get_parent_ids(leaf_node_id, attribute_B_id, brand_type_id, category_type_id)
                                attrib_B_label_ids.append(all_parent_ids)
                        else:
                            attrib_B_label_ids = []
                            for label in labels_B:
                                attrib_B_label_ids.append(label[0])
                        attributeA_to_attributeB.append([attrib_A_label_ids, attrib_B_label_ids])
        del labelled_skus_data
        # pprint(attributeA_to_attributeB)
        atrbA_atrbB_map = defaultdict(list)
        atrbB_atrbA_map = defaultdict(list)
        for atrbA_list, atrbB_list in attributeA_to_attributeB:
            for atrbA in atrbA_list:
                for atrbB in atrbB_list:
                    for i in atrbA:
                        for j in atrbB:
                            atrbA_atrbB_map[i].append(j)
                            atrbB_atrbA_map[j].append(i)
        atrbA_atrbB_freq = {}
        atrbB_atrbA_freq = {}
        for atrbA, atrbB_list in atrbA_atrbB_map.items():
            atrbB_freq = Counter(atrbB_list).most_common()
            # atrbB_freq is a list of 2-element-tuples (unique_atrbB_value, atrbB_freq)
            atrbA_atrbB_freq[atrbA] = atrbB_freq
        for atrbB, atrbA_list in atrbB_atrbA_map.items():
            atrbA_freq = Counter(atrbA_list).most_common()
            atrbB_atrbA_freq[atrbB] = atrbA_freq
        atrbA_atrbB_score = {}
        atrbB_atrbA_score = {}
        for atrbA, atrbB_list in atrbA_atrbB_freq.items():
            n_unique_atrbB = len(atrbB_list)
            atrbB_score = []
            count_atrbB = 0
            for atrbB in atrbB_list:
                # atrbB[0] is the id and atrbB[1] is the freqency
                count_atrbB += atrbB[1]
            for atrbB in atrbB_list:
                score = atrbB[1]/count_atrbB
                atrbB_score.append((atrbB[0], score))
            atrbA_atrbB_score[atrbA] = atrbB_score
        for atrbB, atrbA_list in atrbB_atrbA_freq.items():
            atrbA_score = []
            count_atrbA = 0
            for atrbA in atrbA_list:
                count_atrbA += atrbA[1]
            for atrbA in atrbA_list:
                score = atrbA[1]/count_atrbA
                atrbA_score.append((atrbA[0], score))
            atrbB_atrbA_score[atrbB] = atrbA_score
        json.dump(atrbA_atrbB_score, AtoB_output_fo, indent=4, sort_keys=True)
        json.dump(atrbB_atrbA_score, BtoA_output_fo, indent=4, sort_keys=True)


def get_parent_ids(node_id, class_id, brand_type_id, category_type_id):
    global all_parent_ids
    all_parent_ids.append(node_id)
    if class_id == brand_type_id:
        graph = inp.BM_graph
    elif class_id == category_type_id:
        graph = inp.CM_graph
    node_map = graph.node[node_id]
    if 'Parents' not in node_map:
        return
    else:
        parent_ids = graph.predecessors(node_id)
        for id in parent_ids:
            get_parent_ids(id, class_id, brand_type_id, category_type_id)


def main():
    inp.load_config_file()
    inp.load_class_info_files()
    inp.get_attribute_ids()
    inp.load_master_files()
    used_ids = []
    for v in inp.attributes_data['references'].values():
        attribute_A_id = v['type_id']
        used_ids.append(attribute_A_id)
        for v in inp.attributes_data['references'].values():
            attribute_B_id = v['type_id']
            if (attribute_B_id != attribute_A_id) and (attribute_B_id not in used_ids):
                get_attribute_association(attribute_A_id, attribute_B_id)


if __name__ == '__main__':
    main()



