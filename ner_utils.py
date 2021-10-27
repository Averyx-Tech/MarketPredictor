
"""
    Type: Utility function
    Developer: Vignesh
    Description: Supporting function for ner_bert.py
"""

def get_tags(input):
    next = 0
    tag = ''
    entity = ''
    record = {'ORG_E': [], 'PER_E': [], 'LOC_E': [], 'MISC_E': []}
    entities = []
    cat = {'ORG': 0, 'PER': 0, 'LOC': 0, 'MISC': 0}

    for pred in input:
        w = pred['word']
        pred_tag = pred['tag']
        
        if w != "PADword":
            if pred_tag[0] == "B":
                if next == 1:
                    record[tag+'_E'].append(entity)
                    cat[tag] = 1
                else:
                    tag = pred_tag[2:]
                    #print(tag)
                    entity = w
                    next = 1

            elif pred_tag[0] == 'I':
                entity += ' '+w
            else:
                next = 0

            if next == 0 and tag != '':
                #print(tag)
                record[tag+'_E'].append(entity) 
                cat[tag] = 1
                tag = ''
    if next == 1:
        record[tag+'_E'].append(entity) 

    for key in record:
        record[key] = list(set(record[key]))

    return record, cat