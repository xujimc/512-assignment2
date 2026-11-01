import os, re, sys, csv, statistics as st

WARMUP = 5

def pct(xs, p):
    xs = sorted(xs)
    if not xs:
        return float('nan')
    k = (len(xs) - 1) * p / 100
    f = int(k)
    c = min(f + 1, len(xs) - 1)
    return xs[f] + (xs[c] - xs[f]) * (k - f)

def load_run(d):
    status = open(os.path.join(d, 'status.txt')).read().strip()
    m = re.search(r'N=(\d+) interval=(\d+) moves=(\d+) seed=(\S+) status=(\S+)', status)
    n, iv, mv, seed, ok = int(m[1]), int(m[2]), int(m[3]), m[4], m[5]
    done = {}
    decided = {}
    for i in range(1, n + 1):
        done[i] = []
        decided[i] = []
        path = os.path.join(d, f'perf-{i}.txt')
        if not os.path.exists(path):
            continue
        for line in open(path):
            p = line.split()
            k = p.index('PERF')
            if p[k + 1] == 'DONE':
                mid, s, e, att, to = p[k + 2], int(p[k + 3]), int(p[k + 4]), int(p[k + 5]), int(p[k + 6])
                seq = int(mid.split('#')[1])
                done[i].append((seq, s, e, att, to))
            elif p[k + 1] == 'DECIDED':
                proc = int(re.search(r':40(\d)\d\d#', p[k + 4])[1])
                decided[i].append((int(p[k + 2]), int(p[k + 3]), proc))
    return dict(n=n, iv=iv, mv=mv, seed=seed, ok=ok, done=done, decided=decided)

def run_metrics(r):
    lats, atts, tos = [], [], 0
    for i, rows in r['done'].items():
        for seq, s, e, att, to in rows:
            if seq <= WARMUP:
                continue
            lats.append((e - s) / 1e6)
            atts.append(att)
            tos += to
    times = sorted(t for _, t, _ in r['decided'][1])
    t10, t90 = pct(times, 10), pct(times, 90)
    cnt = sum(1 for t in times if t10 <= t <= t90)
    thr = (cnt - 1) / ((t90 - t10) / 1e9) if t90 > t10 else float('nan')
    return dict(lat_mean=st.mean(lats), lat_p50=pct(lats, 50), lat_p95=pct(lats, 95),
                att_mean=st.mean(atts), timeouts=tos, thr=thr, streak=streak(r))

def window(r):
    dec = sorted(r['decided'][1])
    n = r['n']
    last = {}
    for slot, t, proc in dec:
        last[proc] = t
    t0 = dec[min(5 * n, len(dec) - 1)][1]
    t1 = min(last.values())
    return [x for x in dec if t0 <= x[1] <= t1], t0, t1

def streak(r):
    w, _, _ = window(r)
    if not w:
        return float('nan')
    runs, prev = 0, None
    for _, _, proc in w:
        if proc != prev:
            runs += 1
            prev = proc
    return len(w) / runs

def per_process(r):
    w, t0, t1 = window(r)
    span = (t1 - t0) / 1e9
    cnt = {i: 0 for i in r['done']}
    for _, _, proc in w:
        cnt[proc] += 1
    out = {}
    for i, rows in r['done'].items():
        steady = [((e - s) / 1e6, a) for seq, s, e, a, _ in rows if seq > WARMUP]
        ls = [x for x, _ in steady]
        out[i] = dict(rate=cnt[i] / span, share=cnt[i] / max(1, len(w)), lat=st.mean(ls), lat_p95=pct(ls, 95),
                      att=st.mean(a for _, a in steady), window_s=span, streak=streak(r), interval=r['iv'])
    return out

def main(data):
    rows = []
    sd = os.path.join(data, 'sweep')
    for name in sorted(os.listdir(sd)):
        r = load_run(os.path.join(sd, name))
        m = run_metrics(r)
        rows.append(dict(n=r['n'], interval=r['iv'], seed=r['seed'], status=r['ok'], **m))
    with open(os.path.join(data, 'sweep.csv'), 'w', newline='') as f:
        w = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
        w.writeheader()
        w.writerows(rows)
    frows = []
    fd = os.path.join(data, 'fair2')
    if os.path.isdir(fd):
        for name in sorted(os.listdir(fd)):
            if not os.path.exists(os.path.join(fd, name, 'status.txt')):
                continue
            r = load_run(os.path.join(fd, name))
            for i, v in per_process(r).items():
                frows.append(dict(n=r['n'], seed=r['seed'], status=r['ok'], proc=i, **v))
        with open(os.path.join(data, 'fair.csv'), 'w', newline='') as f:
            w = csv.DictWriter(f, fieldnames=list(frows[0].keys()))
            w.writeheader()
            w.writerows(frows)
    print(len(rows), 'sweep runs,', len(frows), 'fairness rows')

if __name__ == '__main__':
    main(sys.argv[1])
