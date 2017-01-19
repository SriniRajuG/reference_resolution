from __future__ import division
from __future__ import print_function
import json
import sys
import csv
from io import open
import copy

import inp_files as inp
import msmnt

# PD - Product Description
# BM - Brand Master
# CM - Category Master
# MM - Measurement Master


def main():
    config_data = inp.load_config_file()
    attributes_data = inp.get_attribute_ids(config_data)
    class_info_data = inp.load_class_info_files(config_data)
    edit_dist_params = inp.load_edit_dist_params(config_data)
    BM_graph, brand_nodes, sub_brand_nodes, CM_graph, MM = inp.load_master_files(config_data, attributes_data)
    special_chars = config_data['special_chars'].encode('ascii', errors='ignore')
    id_to_label_func_map = inp.make_attrib_ids_to_label_func_map(attributes_data, BM_graph, brand_nodes,
                                                                 sub_brand_nodes, CM_graph)
    try:
        output_ids_fo = open(config_data['output_ids'], 'wb')
        output_csv_fo = open(config_data['output_csv'], 'wb')
    except IOError as e:
        print(e)
        sys.exit()
    else:
        output_writer = csv.writer(output_csv_fo)
        unlabelled_SKUs_filename = config_data['unlabelled_SKUs']
        raw_data = inp.load_unlabelled_SKUs(unlabelled_SKUs_filename)
        labels = []
        SKU_counter = 0
        total_SKUs = len(raw_data)
        msmnt_type_id = attributes_data['references']['measurement']['type_id']
        for sku_map in raw_data:  # *raw_data* is a list of maps. Each map corresponds to one SKU
            sku_id = sku_map['id']
            SKU_counter += 1
            if not SKU_counter % 500:
                print(str(SKU_counter) + ' ' + 'of' + ' ' + str(total_SKUs))
            if 'segment' in sku_map:
                segment_ids = sku_map['segment']
                if segment_ids:  # segment_ids is not empty
                    segment_id = sku_map['segment'][0]  # <----- currently taking only one segment.Should take a list
                    if segment_id in class_info_data and segment_id in config_data:
                        for uuid, name in class_info_data[segment_id]['attributes'].items():
                            if name == attributes_data['attributes']['product_name']:
                                PD_id = uuid
                        attrib_ids = copy.copy(config_data[segment_id]['attribs'])
                        sku_edit_dist_params = edit_dist_params[segment_id]
                        if PD_id in sku_map:
                            attrib_id_to_solution_map = dict()
                            attrib_id_to_solution_map['id'] = sku_id
                            # sku_map[PD_id] must be a one element list. So 0 in sku_map[PD_id][0]
                            PD_lower_case = sku_map[PD_id][0].strip().lower().encode('ascii', errors='ignore')
                            phrase_to_label = PD_lower_case.translate(None, special_chars)
                            if msmnt_type_id in attrib_ids:
                                phrase_to_label, matched_msmnt_ids = msmnt.remove_label_msmnt(phrase_to_label, MM)
                                attrib_id_to_solution_map[msmnt_type_id] = matched_msmnt_ids
                                attrib_ids.remove(msmnt_type_id)
                            ngrams_map = inp.make_ngrams(phrase_to_label.split())
                            unlabelled_tokens_sets = list()  # list of sets
                            for atrb_id in attrib_ids:
                                func = id_to_label_func_map[atrb_id]['name']
                                params = id_to_label_func_map[atrb_id]['param']
                                obj_ref_id = get_obj_ref_id(atrb_id, segment_id, class_info_data, attributes_data)
                                skuid, matched_nodes, unlabelled_tokens = func((sku_id, ngrams_map),
                                                                               sku_edit_dist_params, *params)
                                # skuid: being done for platform integration (Spark).
                                attrib_id_to_solution_map[obj_ref_id] = matched_nodes
                                unlabelled_tokens_sets.append(unlabelled_tokens)
                            labels.append(attrib_id_to_solution_map)
                            unlabel_tokens = set.intersection(*unlabelled_tokens_sets)
                            get_label_names(phrase_to_label, attrib_id_to_solution_map, output_writer, segment_id,
                                            class_info_data, attributes_data, BM_graph, CM_graph, unlabel_tokens)
                        else:
                            pass
                    else:
                        pass
        json_string = json.dumps(labels)
        output_ids_fo.write(json_string.encode('utf-8'))


def get_obj_ref_id(type_id, segment_id, class_info_data, attributes_data):
    a = {}
    for obj_ref_id, ci_val in class_info_data[segment_id]['references'].items():
        if ci_val[1] == type_id:
            a[obj_ref_id] = ci_val[0]
    for k, v in attributes_data['references'].items():
        if v['type_id'] == type_id:
            po = v['regex']
    for k, v in a.items():
        if po.search(v):
            return k


def get_label_names(unlabelled_phrase, attrib_id_to_soln_map, output_writer, segment_id, class_info_data,
                    attributes_data, BM_graph, CM_graph, unlabel_tokens):
    for obj_ref_id, solns in attrib_id_to_soln_map.items():
        for k, v in class_info_data[segment_id]['references'].items():
            if k == obj_ref_id:
                attrib_id = v[1]
                if attrib_id == attributes_data['references']['brand']['type_id']:
                    brand_label_ids = solns
                elif attrib_id == attributes_data['references']['category']['type_id']:
                    category_label_ids = solns
    brand_names = [[]]*len(brand_label_ids)
    for i in range(len(brand_label_ids)):
        leaf_id = brand_label_ids[i][0]
        global brand_label
        brand_label = []
        brand_label.append(BM_graph.node[leaf_id]['Name'])
        get_parent_name(leaf_id, BM_graph)
        brand_label.reverse()
        brand_names[i] = brand_label
    category_names = [[]]*len(category_label_ids)
    for i in range(len(category_label_ids)):
        leaf_id = category_label_ids[i][0]
        category_names[i] = CM_graph.node[leaf_id]['Name']
    u_sku_id = attrib_id_to_soln_map['id'].encode('utf-8')
    u_unlabelled_phrase = unlabelled_phrase.encode('utf-8')
    u_brand_names = []
    if brand_names:
        for i in brand_names:
            a = []
            for j in i:
                a.append(j.encode('utf-8'))
            u_brand_names.append(a)
    u_category_names = [i.encode('utf-8') for i in category_names]
    u_unlabel_tokens = [i.encode('utf-8') for i in unlabel_tokens]
    full_list = [u_sku_id] + [u_unlabelled_phrase] + [u_unlabel_tokens] + [u_brand_names] + [u_category_names]
    output_writer.writerow(full_list)


def get_parent_name(leaf_id, BM_graph):
    if 'Parents' not in BM_graph.node[leaf_id]:
        return
    parent_id = BM_graph.predecessors(leaf_id)[0]  # There can only be one predecessor for each node
    global brand_label
    brand_label.append(BM_graph.node[parent_id]['Name'])
    get_parent_name(parent_id, BM_graph)


if __name__ == '__main__':
    main()
