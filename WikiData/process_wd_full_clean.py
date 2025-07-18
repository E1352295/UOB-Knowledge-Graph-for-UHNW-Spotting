#!/usr/bin/env python3
"""
process_wd.py – Wikidata ETL for n8n  (2025-07-07 修订)
-----------------------------------------------------
变更要点：
* fix: 回传 payload 之前同步去重 edges，解决 n8n 输出中重复关系 (#dup-edges)
* 其余逻辑保持上一版修复：主/关系节点属性隔离、shared_state 全 true、最终文件再去重
"""

import sys, os, json, argparse
from pathlib import Path
from collections import defaultdict

prs = argparse.ArgumentParser(add_help=False)
prs.add_argument('--reset', action='store_true')
prs.add_argument('--state-file')
prs.add_argument('--data-file')
args, _ = prs.parse_known_args()

STATE_FILE = Path(args.state_file or os.getenv('WD_STATE_FILE', './shared_state.json')).expanduser().resolve()
DATA_FILE  = Path(args.data_file  or os.getenv('WD_DATA_FILE',  './data.json')).expanduser().resolve()

# ------------------------------------------------------------------
# helpers: load / save state
# ------------------------------------------------------------------
def _load_state():
    if args.reset:
        _save_state({'seen': {}})  # Overwrite with a fresh, empty state

    if not STATE_FILE.exists():
        return {'seen': {}}
    try:
        return json.loads(STATE_FILE.read_text())
    except Exception:
        STATE_FILE.rename(STATE_FILE.with_suffix('.bak'))
        return {'seen': {}}

def _save_state(state: dict):
    STATE_FILE.parent.mkdir(parents=True, exist_ok=True)
    tmp = STATE_FILE.with_suffix('.tmp')
    tmp.write_text(json.dumps(state, indent=2, ensure_ascii=False))
    tmp.replace(STATE_FILE)

# ------------------------------------------------------------------
# helpers: load / save data
# ------------------------------------------------------------------
def _load_data():
    if args.reset:
        # Overwrite with a fresh, empty data file
        empty_data = {'persons': [], 'edges': []}
        DATA_FILE.write_text(json.dumps(empty_data, indent=2, ensure_ascii=False))

    if not DATA_FILE.exists():
        return {'persons': [], 'edges': []}
    raw = json.loads(DATA_FILE.read_text())
    for p in raw.get('persons', []):
        for fld in ('props', 'attributes'):
            if fld not in p or not isinstance(p[fld], dict):
                p[fld] = {}
            for k, v in list(p[fld].items()):
                if not isinstance(v, list):
                    p[fld][k] = [v] if v is not None else []
    return raw

def _dedup_edges(in_edges: list[dict]) -> list[dict]:
    """内存去重：seed|rel|relType 组成唯一键"""
    uniq = {}
    for e in in_edges:
        key = (e.get('seed'), e.get('rel'), e.get('relType'))
        if None not in key:
            uniq[key] = {'seed': key[0], 'rel': key[1], 'relType': key[2]}
    return list(uniq.values())

def _save_data(data: dict):
    """写盘前二次去重 & defaultdict → dict"""
    data['edges'] = _dedup_edges(data['edges'])

    norm_persons = []
    for p in data['persons']:
        p_out = {'id': p['id'], 'props': {}, 'attributes': {}}
        for sec in ('props', 'attributes'):
            for k, v in p[sec].items():
                if not isinstance(v, list):
                    v = [v] if v is not None else []
                uniq = list(dict.fromkeys(v))
                p_out[sec][k] = uniq[0] if len(uniq) == 1 else uniq
        norm_persons.append(p_out)
    data['persons'] = norm_persons

    DATA_FILE.parent.mkdir(parents=True, exist_ok=True)
    tmp = DATA_FILE.with_suffix('.tmp')
    tmp.write_text(json.dumps(data, indent=2, ensure_ascii=False))
    tmp.replace(DATA_FILE)

# ---------------------------------------------------------------------------
# 工具函数
# ---------------------------------------------------------------------------
def _qid_from_uri(uri: str | None):
    return uri.split('/')[-1] if uri else None

def _get_value(obj, key='value'):
    return obj.get(key) if isinstance(obj, dict) else None

# ---------------------------------------------------------------------------
# Person 聚合
# ---------------------------------------------------------------------------
def _add_person(pool: dict, uri: str | None, row: dict, rel_label_key: str | None):
    if not uri:
        return None
    qid = _qid_from_uri(uri)
    if not qid:
        return None

    row_person_qid = _qid_from_uri(_get_value(row.get('person'))) if row.get('person') else None
    is_main = (row_person_qid == qid)

    if qid not in pool:
        pool[qid] = {'id': qid,
                     'props': defaultdict(list),
                     'attributes': defaultdict(list)}
    node = pool[qid]

    if is_main:
        node['props'].clear()
        node['attributes'].clear()
        node['props'] = defaultdict(list)
        node['attributes'] = defaultdict(list)

        prop_map = {
            'name'          : 'personLabel',
            'dateOfBirth'   : 'dateOfBirth',
            'citizenship'   : 'citizenshipLabel',
            'officialWebsite': 'officialWebsite',
            'netWorth'      : 'netWorth',
            'occupation'    : 'occupationLabel',
            'employer'      : 'employerLabel',
            'positionHeld'  : 'positionHeldLabel',
        }
        for p_name, row_key in prop_map.items():
            val = _get_value(row.get(row_key))
            if val and val not in node['props'][p_name]:
                node['props'][p_name].append(val)

        std_labels = set(prop_map.values()) | {'personLabel'}
        for k, v_obj in row.items():
            if k.endswith('Label') and k not in std_labels:
                val = _get_value(v_obj)
                if val and val not in node['attributes'][k]:
                    node['attributes'][k].append(val)
        return node

    label_key = rel_label_key or 'personLabel'
    rel_name  = _get_value(row.get(label_key))
    if rel_name and rel_name not in node['props']['name']:
        node['props']['name'].append(rel_name)
    return node

# ---------------------------------------------------------------------------
# 主流程
# ---------------------------------------------------------------------------
def main():
    state = _load_state()
    seen  = state.setdefault('seen', {})
    data  = _load_data()

    persons_pool = {p['id']: {'id': p['id'],
                              'props': defaultdict(list, p['props']),
                              'attributes': defaultdict(list, p['attributes'])}
                    for p in data.get('persons', [])}
    edges        = data.get('edges', [])

    try:
        items = json.load(sys.stdin)
    except json.JSONDecodeError as e:
        sys.exit(f'❌ Invalid JSON {e}')

    for itm in items:
        seed_qid = itm.get('qid')
        bindings = itm.get('results', {}).get('bindings', []) or itm.get('bindings', [])

        if not seed_qid and bindings:
            seed_qid = _qid_from_uri(_get_value(bindings[0].get('person')))
        if seed_qid:
            seen[seed_qid] = True

        if not bindings:
            continue

        for row in bindings:
            seed_uri = _get_value(row.get('person'))
            seed_node = _add_person(persons_pool, seed_uri, row, None)
            if not seed_node:
                continue

            rel_map = {
                'spouse'  : 'SPOUSE_OF',
                'father'  : 'FATHER_OF',
                'mother'  : 'MOTHER_OF',
                'child'   : 'CHILD_OF',
                'sibling' : 'SIBLING_OF',
                'relative': 'RELATIVE_OF',
            }
            for col, typ in rel_map.items():
                if col not in row:
                    continue
                tgt_uri = _get_value(row[col])
                tgt     = _add_person(persons_pool, tgt_uri, row, f'{col}Label')
                if not tgt:
                    continue

                if seed_node['id'] == tgt['id']:
                    continue

                edges.append({'seed': seed_node['id'],
                              'rel' : tgt['id'],
                              'relType': typ})
                if tgt['id'] not in seen:
                    seen[tgt['id']] = False

    # ------- 去重 edges（内存 & 文件）-------
    edges = _dedup_edges(edges)

    # ------- 写盘 -------
    _save_state(state)
    _save_data({'persons': list(persons_pool.values()), 'edges': edges})

    next_q = [{'qid': q} for q, done in seen.items() if not done]

    json.dump({
        # 'payload': {'persons': list(persons_pool.values()),
        #                    'edges'  : edges},
               'next'   : next_q},
              sys.stdout, indent=2, ensure_ascii=False)

if __name__ == '__main__':
    main()
