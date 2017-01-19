def remove_label_msmnt(phrase, msmnt_master):
    matched_msmnt_ids = set()
    for msmnt_id, msmnt_regex_objs in msmnt_master.items():
        for regex_obj in msmnt_regex_objs:
            match_obj = regex_obj.search(phrase)
            if match_obj:
                match_str = match_obj.group(0)
                phrase = phrase.replace(match_str, '')
                matched_msmnt_ids.add(msmnt_id)
    return phrase, list(matched_msmnt_ids)
