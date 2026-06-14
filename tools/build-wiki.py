import re, os
SRC="docs/architecture/ARCHITECTURE.md"
WIKI="wiki"; os.makedirs(WIKI, exist_ok=True)
raw=open(SRC).read()
raw=re.sub(r'^<!--.*?-->\s*', '', raw, count=1, flags=re.S)
lines=raw.split('\n')

def slug(h):
    s=re.sub(r'`','',h)
    s=re.sub(r'\[([^\]]+)\]\([^)]+\)', r'\1', s)   # link text only
    s=re.sub(r'[^\w\s-]','',s).strip().lower()
    return re.sub(r'[-\s]+','-',s)

# split at H2
idx=[i for i,l in enumerate(lines) if re.match(r'^## ', l)]
pre=lines[:idx[0]]
sections=[]
for k,start in enumerate(idx):
    end=idx[k+1] if k+1<len(idx) else len(lines)
    title=re.sub(r'^##\s+','',lines[start]).strip()
    sections.append((title, lines[start:end]))

# index.md = hand-authored quick overview (source: docs/architecture/overview.md);
# the in-doc "How to read" section is skipped — the sidebar + overview replace it.
overview_md = open('docs/architecture/overview.md').read()
pages=[]   # (title, filename, block)
for title,block in sections[1:]:
    pages.append((title, slug(title)+'.md', block))

# anchor -> file map (every heading slug)
amap={}
for title,fn,block in pages:
    for l in block:
        m=re.match(r'^#{1,6}\s+(.*)$', l)
        if m: amap[slug(m.group(1))]=fn
    # also the H1 in index
if pre:
    for l in pre:
        m=re.match(r'^#\s+(.*)$', l)
        if m: amap[slug(m.group(1))]='index.md'

def rewrite(block, selffn):
    out=[]
    infence=False
    for l in block:
        if l.strip().startswith('```'): infence=not infence
        if not infence:
            def repl(mm):
                norm=re.sub(r'-{2,}','-', mm.group(1))   # GitHub double-dash -> mkdocs single-dash
                tgt=amap.get(norm)
                if not tgt: return mm.group(0)
                return f'](#{norm})' if tgt==selffn else f']({tgt}#{norm})'
            l=re.sub(r'\]\(#([a-z0-9-]+)\)', repl, l)
        out.append(l)
    return '\n'.join(out)

for title,fn,block in pages:
    open(os.path.join(WIKI,fn),'w').write(rewrite(block, fn).strip()+'\n')
open(os.path.join(WIKI,'index.md'),'w').write(rewrite(overview_md.split('\n'),'index.md').strip()+'\n')

# build nav
SERVICES={'Submission API (Go)','Build Pipeline & Sandbox Orchestrator (Go)','Bot-Fleet Controller (Go)',
 'Bot-Fleet Load Generator (Rust)','eBPF Latency Capture (Rust)','Telemetry Ingester & Rollup (Rust)',
 'Correctness Validator (Go)','Score Computer & Leaderboard API (Go)','Platform Foundations: Auth, Shared Libraries & Schemas'}
nav=["  - Overview: index.md"]
svc=[]; appx=[]; top=[]
for title,fn,block in pages:
    if title in SERVICES: svc.append((title,fn))
    elif title.startswith('Appendix'): appx.append((title,fn))
    else: top.append((title,fn))
# emit in document order, inserting Services group where the first service appears, appendix at end
emitted_svc=emitted_appx=False
order=[(t,f) for t,f,_ in pages]
for t,f in order:
    if t in SERVICES:
        if not emitted_svc:
            nav.append("  - Services:"); 
            for st,sf in svc: nav.append(f'      - "{st}": {sf}')
            emitted_svc=True
    elif t.startswith('Appendix'):
        if not emitted_appx:
            nav.append("  - Appendix:")
            for at,af in appx: nav.append(f'      - "{at}": {af}')
            emitted_appx=True
    else:
        nav.append(f'  - "{t}": {f}')
navtext="\n".join(nav)

mkdocs=f'''site_name: Match-Bench Architecture
site_description: Fair, un-gameable benchmarking for high-frequency-trading algorithms
site_url: https://agrawalx.github.io/match-bench/
repo_url: https://github.com/agrawalx/match-bench
repo_name: agrawalx/match-bench
docs_dir: wiki
theme:
  name: material
  palette:
    - scheme: default
      primary: indigo
      toggle: {{ icon: material/weather-night, name: Dark mode }}
    - scheme: slate
      primary: indigo
      toggle: {{ icon: material/weather-sunny, name: Light mode }}
  features:
    - navigation.sections
    - navigation.top
    - navigation.tracking
    - toc.follow
    - content.code.copy
    - search.suggest
    - search.highlight
markdown_extensions:
  - admonition
  - attr_list
  - md_in_html
  - tables
  - toc:
      permalink: true
  - pymdownx.highlight:
      anchor_linenums: false
  - pymdownx.inlinehilite
  - pymdownx.superfences:
      custom_fences:
        - name: mermaid
          class: mermaid
          format: !!python/name:pymdownx.superfences.fence_code_format
nav:
{navtext}
'''
open("mkdocs.yml","w").write(mkdocs)
print("pages:",len(pages),"| nav lines:",len(nav))
print("files:", sorted(os.listdir(WIKI)))
