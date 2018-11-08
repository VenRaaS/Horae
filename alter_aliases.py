import logging
import json
import requests
import datetime

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s', datefmt='%Y-%m-%d %I:%M:%S')

LB_ES_HOSTS = 'es-node-01'

INDEX_CATS_2_RESERVED_DAYS = {
    'oua': 14,
    'opp': 14, 
    'gocc': 3,
    'mod': 3,
}

#-- ES APIs
URL_VENRAAS_COMS = 'http://{h}:9200/venraas/_search?sort=update_dt:desc&size=1'
URL_DISTINCT_INDICES = 'http://{h}:9200/{cn}_{cat}_*/_search?size=0'
JSON_DISTINCT_INDICES = {"aggs":{"index_list":{"terms":{"field":"_index","size":100}}}}

URL_DELETE_INDICE = 'http://{h}:9200/{idx}'
URL_COUNT_INDICE = 'http://{h}:9200/{idx}/_count'

URL_ALIASES = 'http://{h}:9200/_aliases'
JSON_ADD_RM_ALIAS = {"actions":[{"add":{"index":"{newidx}","alias":"{ali}"}},{"remove":{"index":"{oldidx}","alias":"{ali}"}}]}
JSON_ADD_ALIAS = {"actions":[{"add":{"index":"{idx}","alias":"{cn}_{cat}"}}]}
JSON_ADD_ALIASES = {"actions":[{"add":{"index":"{cn}_{cat}_*","alias":"{cn}_{cat}"}}]}



def get_sorted_indices(alias, sort_decending=True):
    indices = []
 
    resp = requests.get(URL_ALIASES.format(h=LB_ES_HOSTS))
    iaInfoJson = json.loads(resp.text)
    indices = filter(lambda k: alias in k, iaInfoJson.keys())

    if sort_decending:
        indices.sort(reverse=True)
    else:
        indices.sort()

    return (indices, iaInfoJson)


if '__main__' == __name__:
    url = URL_VENRAAS_COMS.format(h=LB_ES_HOSTS)
    resp = requests.get(url)
    raasJson = json.loads(resp.text)
    coms  = raasJson['hits']['hits'][0]['_source']['companies']
    for com in coms:
        if not com['enabled'] and \
           not com['code_name']:
            continue

        codename = com['code_name']
        
        for cate in INDEX_CATS_2_RESERVED_DAYS.keys():
            alias = '{cn}_{cat}'.format(cn=codename, cat=cate)

            indices, iaInfoJson = get_sorted_indices(alias)
            if len(indices) <= 0:
                logging.warn('none of indices with prefix {0}'.format(alias))
                continue

            #-- the latest index
            idx_latest = indices[0]

            #-- find the latest Aliased index
            #   iaInfoJson, e.g. {"$CN_gocc_20181107":{"aliases":{"$CN_gocc":{}}}, "pchome_gocc_20181105":{"aliases":{}}, ...}
            alias_latest = next((i for i in indices if alias in iaInfoJson[i]['aliases']), None)

            #-- alter aliases
            if cate in ['oua', 'opp']:
                if None == alias_latest or \
                   alias_latest != idx_latest:
                    JSON_ADD_ALIASES['actions'][0]['add']['index'] = '{cn}_{cat}_*'.format(cn=codename, cat=cate)
                    JSON_ADD_ALIASES['actions'][0]['add']['alias'] = '{cn}_{cat}'.format(cn=codename, cat=cate)
                    url = URL_ALIASES.format(h=LB_ES_HOSTS)
                    resp = requests.post(url, json=JSON_ADD_ALIASES)
                    logging.info('{0} --data {1}, {2}'.format(url, JSON_ADD_ALIASES, resp.text))
                else:
                    logging.info('{0}(idx) = {1}(alias), latest indices are equal, awesome +1'.format(idx_latest, alias_latest))
            elif cate in ['mod', 'gocc']:
                if None == alias_latest:
                    # newborn, none alias yet
                    logging.info('newborn, none alias yet')
                    logging.info('let\'s alias the latest one {0} ...'.format(idx_latest))
                    JSON_ADD_ALIAS['actions'][0]['add']['index'] = idx_latest
                    JSON_ADD_ALIAS['actions'][0]['add']['alias'] = alias
                    url = URL_ALIASES.format(h=LB_ES_HOSTS)
                    resp = requests.post(url, json=JSON_ADD_ALIAS)
                    logging.info('{0} --data {1}, {2}'.format(url, JSON_ADD_ALIAS, resp.text))
                elif alias_latest != idx_latest:
                    # the lastest alias != the latest index
                    logging.info('{0}(idx) <> {1}(alias), latest indices is not equal'.format(idx_latest, alias_latest))
                    logging.info('let\'s sync the alias to the latest index ...')
                    url = URL_COUNT_INDICE.format(h=LB_ES_HOSTS, idx=idx_latest)
                    resp = requests.get(url)
                    cnt_idx = float(json.loads(resp.text)['count'])
                    url = URL_COUNT_INDICE.format(h=LB_ES_HOSTS, idx=alias_latest)
                    resp = requests.get(url)
                    cnt_alias = float(json.loads(resp.text)['count'])
                    ratio = min(cnt_idx,cnt_alias) / max(cnt_idx,cnt_alias)
                    if 0.98 <= min(cnt_idx,cnt_alias) / max(cnt_idx,cnt_alias):
                        logging.info('0.98 < {0}, valid ratio.'.format(ratio))
                        # json command
                        JSON_ADD_RM_ALIAS['actions'][0]['add']['index'] = idx_latest
                        JSON_ADD_RM_ALIAS['actions'][0]['add']['alias'] = alias 
                        JSON_ADD_RM_ALIAS['actions'][1]['remove']['index'] = alias_latest 
                        JSON_ADD_RM_ALIAS['actions'][1]['remove']['alias'] = alias
                        url = URL_ALIASES.format(h=LB_ES_HOSTS)
                        resp = requests.post(url, json=JSON_ADD_RM_ALIAS)
                        logging.info('{0} --data {1}, {2}'.format(url, JSON_ADD_RM_ALIAS, resp.text))
                    else:
                        logging.warn('{0}/{1}= {2} < 0.98 unable to alter alias due to invalid ratio.'.format( \
                            min(cnt_idx,cnt_alias), max(cnt_idx,cnt_alias), ratio))
                else:
                    logging.info('{0}(idx) = {1}(alias), latest indices are equal, awesome +1'.format(idx_latest, alias_latest))
                 
            #-- purge indices
            indices, iaInfoJson = get_sorted_indices(alias)
            if len(indices) <= 0:
                logging.warn('none of indices with prefix {0}'.format(alias))
                continue
            
            indices_aliasbeg = []
            for i, idx in enumerate(indices):
                if alias in iaInfoJson[idx]['aliases']:
                    indices_aliasbeg = indices[i: ]
                    break

            revdays = INDEX_CATS_2_RESERVED_DAYS[cate]
            if len(indices_aliasbeg) <= revdays:
                logging.info('[{0}] has {1} indices and the latest alias on [{2}], <= {3}, which does not need to purge yet.'.format(alias, len(indices_aliasbeg), indices_aliasbeg[0], revdays))
            else:
                idx_end = revdays - len(indices_aliasbeg)
#                ymd = idx_latest.split('_')[-1]
#                dt_latest = datetime.datetime.strptime(ymd, '%Y%m%d')
#                dt_end =  dt_latest - datetime.timedelta(days=revdays)
                for idx in indices_aliasbeg[idx_end: ] :
                    urldel = URL_DELETE_INDICE.format(h=LB_ES_HOSTS, idx=idx)
                    resp = requests.delete(urldel)
                    logging.info('delete {0}, {1}'.format(urldel, resp.text))



