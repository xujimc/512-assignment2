import csv, sys, os, statistics as st
from collections import defaultdict

data, out = sys.argv[1], sys.argv[2]
NS = [2, 3, 5, 7, 9]
IVS = [0, 1, 2, 5, 10, 20, 50, 100, 200]

agg = defaultdict(list)
for r in csv.DictReader(open(os.path.join(data, 'sweep.csv'))):
    agg[(int(r['n']), int(r['interval']))].append(r)

def m(n, iv, key):
    return st.mean(float(r[key]) for r in agg[(n, iv)])

def series(key, fmt):
    return '\n'.join('\\addplot coordinates {' + ' '.join(f'({iv},{fmt.format(m(n, iv, key))})' for iv in IVS) + '};' for n in NS)

L = []
def cmd(name, body):
    L.append('\\newcommand{\\%s}{%s}' % (name, body))

cmd('LatMean', series('lat_mean', '{:.2f}'))
cmd('LatPnf', series('lat_p95', '{:.2f}'))
cmd('Thr', series('thr', '{:.1f}'))
cmd('Att', series('att_mean', '{:.3f}'))
cmd('Streak', series('streak', '{:.2f}'))
for iv, nm in ((0, 'Zero'), (10, 'Ten'), (20, 'Twenty')):
    cmd('ThrN' + nm, '\\addplot coordinates {' + ' '.join(f'({n},{m(n, iv, "thr"):.1f})' for n in NS) + '};')

fr = list(csv.DictReader(open(os.path.join(data, 'fair.csv'))))
tab = []
for n in (5, 9):
    for iv in (0, 10):
        rows = [r for r in fr if int(r['n']) == n and int(r['interval']) == iv]
        byp, bys = defaultdict(list), defaultdict(dict)
        for r in rows:
            byp[int(r['proc'])].append(r)
            bys[r['seed']][int(r['proc'])] = float(r['rate'])
        if iv == 10:
            pts = ' '.join(f'({p},{st.mean(float(x["rate"]) for x in v):.2f}) +- (0,{st.stdev(float(x["rate"]) for x in v):.2f})' for p, v in sorted(byp.items()))
            cmd('FairRate' + ('Five' if n == 5 else 'Nine'), '\\addplot+[error bars/.cd, y dir=both, y explicit] coordinates {' + pts + '};')
        jains, bests, worsts = [], [], []
        for s, d in bys.items():
            rs = list(d.values())
            jains.append(sum(rs) ** 2 / (len(rs) * sum(x * x for x in rs)))
            bests.append(max(d, key=d.get))
            worsts.append(min(d, key=d.get))
        strk = st.mean(float(r['streak']) for r in rows)
        att1 = st.mean(float(x['att']) for x in byp[1])
        attn = st.mean(float(x['att']) for x in byp[n])
        lat1 = st.mean(float(x['lat']) for x in byp[1])
        latn = st.mean(float(x['lat']) for x in byp[n])
        r1 = st.mean(float(x['rate']) for x in byp[1])
        rn = st.mean(float(x['rate']) for x in byp[n])
        tab.append(f'{n} & {iv} & {strk:.1f} & {min(jains):.2f}--{max(jains):.2f} & {",".join(map(str, sorted(bests)))} & {r1:.1f} / {rn:.1f} & {lat1:.1f} / {latn:.1f} & {att1:.2f} / {attn:.2f} \\\\')
cmd('FairTable', '\n'.join(tab))
open(out, 'w').write('\n'.join(L) + '\n')
print('\n'.join(tab))
