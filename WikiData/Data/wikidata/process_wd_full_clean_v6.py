#!/usr/bin/env python3
"""
process_wd.py – Wikidata ETL for n8n (2025‑07‑10 亲属冲突整合版)
----------------------------------------------------------------
* 双向边
* 关系字符串显式：is father/mother/parent/child/spouse/sibling/relative of
* 根据 genderLabel 智能推断 parent‑child 方向
* 冲突整合：同一 pair 同时父又母 -> is parent of；父/母 + parent -> 保留父/母
* --reset 保留
"""
import sys, os, json, argparse
from pathlib import Path
from collections import defaultdict

# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------
prs = argparse.ArgumentParser(add_help=False)
prs.add_argument('--reset', action='store_true')
prs.add_argument('--state-file')
prs.add_argument('--data-file')
args, _ = prs.parse_known_args()

STATE_FILE = Path(args.state_file or os.getenv('WD_STATE_FILE', './shared_state.json')).expanduser().resolve()
DATA_FILE  = Path(args.data_file  or os.getenv('WD_DATA_FILE',  './data.json')).expanduser().resolve()

# ---------------------------------------------------------------------------
# Load / save helpers
# ---------------------------------------------------------------------------
def _load_json(p: Path, empty):
    if args.reset or not p.exists():
        return empty
    try:
        return json.loads(p.read_text())
    except Exception:
        p.rename(p.with_suffix('.bak'))
        return empty

def _save_json(p: Path, obj):
    p.parent.mkdir(parents=True, exist_ok=True)
    tmp = p.with_suffix('.tmp')
    tmp.write_text(json.dumps(obj, indent=2, ensure_ascii=False))
    tmp.replace(p)

state = _load_json(STATE_FILE, {'seen': {}})
data  = _load_json(DATA_FILE,  {'persons': [], 'edges': []})

def _dedup_edges(edges):
    return list({(e['seed'], e['rel'], e['relType']): e for e in edges}.values())

# ---------------------------------------------------------------------------
# Utils
# ---------------------------------------------------------------------------
def _qid(uri: str | None):
    return uri.split('/')[-1] if uri else None

def _val(obj, k='value'):
    return obj.get(k) if isinstance(obj, dict) else None

# ---------------------------------------------------------------------------
# Person pool
# ---------------------------------------------------------------------------
pool = {p['id']: {'id': p['id'],
                  'props': defaultdict(list, p['props']),
                  'attributes': defaultdict(list, p['attributes'])}
        for p in data['persons']}

def _add_person(uri, row, label_key):
    if not uri:
        return None
    q = _qid(uri)
    if not q:
        return None
    main = _qid(_val(row.get('person'))) == q
    if q not in pool:
        pool[q] = {'id': q,
                   'props': defaultdict(list),
                   'attributes': defaultdict(list)}
    node = pool[q]
    if main:
        node['props'].clear(); node['attributes'].clear()
        node['props'] = defaultdict(list); node['attributes'] = defaultdict(list)
        prop_map = {
            'name':'personLabel','dateOfBirth':'dateOfBirth',
            'citizenship':'citizenshipLabel','officialWebsite':'officialWebsite',
            'netWorth':'netWorth','occupation':'occupationLabel',
            'employer':'employerLabel','positionHeld':'positionHeldLabel',
            'gender':'genderLabel'
        }
        for k, rk in prop_map.items():
            v = _val(row.get(rk))
            if v and v not in node['props'][k]:
                node['props'][k].append(v)
        std = set(prop_map.values())|{'personLabel'}
        for k,v in row.items():
            if k.endswith('Label') and k not in std:
                vv=_val(v)
                if vv and vv not in node['attributes'][k]:
                    node['attributes'][k].append(vv)
        return node
    # for related nodes
    rel_name = _val(row.get(label_key or 'personLabel'))
    if rel_name and rel_name not in node['props']['name']:
        node['props']['name'].append(rel_name)
    if rel_name and rel_name not in node['attributes'][label_key or 'personLabel']:
        # ensure list guard —— 旧数据可能是纯字符串
        if not isinstance(node['attributes'][label_key or 'personLabel'], list):
            node['attributes'][label_key or 'personLabel'] = [
                node['attributes'][label_key or 'personLabel']
            ]
        node['attributes'][label_key or 'personLabel'].append(rel_name)
    return node

edges = data['edges']
seen  = state.setdefault('seen', {})

# ---------------------------------------------------------------------------
# Read stdin
# ---------------------------------------------------------------------------
try:
    items = json.load(sys.stdin)
except json.JSONDecodeError as e:
    sys.exit(f'JSON error: {e}')

for itm in items:
    seed_qid = itm.get('qid')
    bindings = itm.get('results', {}).get('bindings', []) or itm.get('bindings', [])
    if not seed_qid and bindings:
        seed_qid=_qid(_val(bindings[0].get('person')))
    if seed_qid:
        seen[seed_qid]=True
    for row in bindings:
        seed_uri=_val(row.get('person'))
        seed=_add_person(seed_uri,row,None)
        if not seed: continue

        gender = seed['props'].get('gender',[None])[0]

        rel_map = {
            'spouse':('is spouse of','is spouse of'),
            'father':('is child of','is father of'),
            'mother':('is child of','is mother of'),
            'child' :('is father of' if gender=='male' else 'is mother of' if gender=='female' else 'is parent of',
                      'is child of'),
            'sibling':('is sibling of','is sibling of'),
            'relative':('is relative of','is relative of'),
        }

        for col,(fwd,rev) in rel_map.items():
            if col not in row: continue
            tgt_uri=_val(row[col])
            tgt=_add_person(tgt_uri,row,f'{col}Label')
            if not tgt: continue
            edges.append({'seed':seed['id'],'rel':tgt['id'],'relType':fwd})
            edges.append({'seed':tgt['id'],'rel':seed['id'],'relType':rev})
            if tgt['id'] not in seen: seen[tgt['id']]=False

# ---------------------------------------------------------------------------
# Consolidate conflicting parent edges
# ---------------------------------------------------------------------------
parent_set={'is father of','is mother of','is parent of'}
child_edge='is child of'
combined=defaultdict(lambda: {'fwd':set(), 'rev':set()})
for e in edges:
    combined[(e['seed'],e['rel'])]['fwd'].add(e['relType'])
    combined[(e['rel'],e['seed'])]['rev'].add(e['relType'])

final_edges=[]
for (a,b),groups in combined.items():
    types=groups['fwd']
    rev_types=groups['rev']
    # determine parent relation forward a->b
    par=types & parent_set
    child=types & {child_edge}
    others=types - parent_set - {child_edge}
    selected=None
    if par:
        if 'is father of' in par and 'is mother of' in par:
            selected='is parent of'
        elif 'is father of' in par:
            selected='is father of'
        elif 'is mother of' in par:
            selected='is mother of'
        else:
            selected='is parent of'
    elif child:
        selected='is child of'
    # write selected if exists
    if selected:
        final_edges.append({'seed':a,'rel':b,'relType':selected})
    # write other non‑parent types
    for t in others:
        final_edges.append({'seed':a,'rel':b,'relType':t})

edges=_dedup_edges(final_edges)

# ---------------------------------------------------------------------------
# Save
# ---------------------------------------------------------------------------
# pack persons
persons=[]
for p in pool.values():
    po={'id':p['id'],'props':{},'attributes':{}}
    for sec in ('props','attributes'):
        for k,v in p[sec].items():
            if not isinstance(v, list):
                v = [v]
            vv=list(dict.fromkeys(v))
            po[sec][k]=vv[0] if len(vv)==1 else vv
    persons.append(po)

_save_json(STATE_FILE,state)
_save_json(DATA_FILE,{'persons':persons,'edges':edges})

next_q=[{'qid':q} for q,d in seen.items() if not d]
json.dump({'payload':{'persons':persons,'edges':edges},'next':next_q},
          sys.stdout,indent=2,ensure_ascii=False)