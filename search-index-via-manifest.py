#! /usr/bin/env python
import sys
import argparse
import time
import os
import csv

from sourmash import sourmash_args
from sourmash.logging import notify
from sourmash.index import LazyLoadedIndex
from sourmash.cli.utils import (add_ksize_arg, add_moltype_args,
                                add_picklist_args)
from sourmash.manifest import CollectionManifest
from sourmash.search import prefetch_database


def main():
    p = argparse.ArgumentParser()
    p.add_argument('query')
    p.add_argument('index')
    p.add_argument('manifest')
    p.add_argument('--threshold-bp', type=float, default=0)
    p.add_argument('--output', required=True)
    add_ksize_arg(p, 31)
    add_moltype_args(p)
    args = p.parse_args()

    moltype = sourmash_args.calculate_moltype(args)
    query = sourmash_args.load_query_signature(args.query,
                                               ksize=args.ksize,
                                               select_moltype=moltype)
    notify('loaded query: {}... (k={}, {})', str(query)[:30],
           query.minhash.ksize, query.minhash.moltype)

    ksize = query.minhash.ksize
    moltype = query.minhash.moltype

    with open(args.manifest, newline="") as fp:
        manifest = CollectionManifest.load_from_csv(fp)

    index = LazyLoadedIndex(args.index, manifest)

    query.minhash = query.minhash.flatten()

    results = list(prefetch_database(query, index, args.threshold_bp))
    
    csvout_fp = open(args.output, 'w', newline="")
    fieldnames = ['intersect_bp', 'jaccard',
                  'max_containment', 'f_query_match', 'f_match_query',
                  'match_filename', 'match_name', 'match_md5', 'match_bp',
                  'query_filename', 'query_name', 'query_md5', 'query_bp']

    csvout_w = csv.DictWriter(csvout_fp, fieldnames=fieldnames)
    csvout_w.writeheader()

    for result in results:
        d = dict(result._asdict())
        del d['match']
        del d['query']
        csvout_w.writerow(d)

    csvout_fp.close()


if __name__ == '__main__':
    main()
