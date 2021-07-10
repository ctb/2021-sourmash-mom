#! /usr/bin/env python
import sys
import argparse
import time
import os

from sourmash import sourmash_args
from sourmash.logging import notify
from sourmash.index import LazyLoadedIndex
from sourmash.cli.utils import (add_ksize_arg, add_moltype_args,
                                add_picklist_args)

from libmom import ManifestOfManifests


def main():
    p = argparse.ArgumentParser()
    p.add_argument('query')
    p.add_argument('dblist', nargs='+')
    p.add_argument('--output-prefix')
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

    # load one or more manifests of manifests, and select on them
    moms = []
    sum_matches = 0
    for db in args.dblist:
        print(f"Loading MoM sqlite database {db}...")
        nrows = ManifestOfManifests.nrows(db)
        print(f"{db} contains {nrows} rows total. Running select......")
        start_time = time.time()
        mom = ManifestOfManifests.load_from_sqlite(db, ksize=ksize,
                                                   moltype=moltype)
        end_time = time.time()
        diff_time = end_time - start_time
        print(f"...{len(mom)} matches remaining for '{db}' ({diff_time:.1f}s)")

        sum_matches += len(mom)
        moms.append(mom)

    print("---")

    print(f"loaded {sum_matches} rows total from {len(moms)} databases.")

    # CTB
    distinct = set()
    for mom in moms:
        for idx_location, manifest in mom.index_locations_and_manifests():
            for row in manifest.rows:
                tup = (row['name'], row['md5'])
                distinct.add(tup)
    print(f"There are {len(distinct)} distinct rows across all MoMs.")
    num_distinct = len(distinct)

    print("---")

    for mom in moms:
        # get the index locations,
        for idx_location, manifest in mom.index_locations_and_manifests():
            prefix = args.output_prefix
            idx_base = os.path.basename(idx_location)
            output_picklist = f"{prefix}.{idx_base}.csv"

            print('writing', output_picklist)
            with open(output_picklist, 'w', newline="") as fp:
                manifest.write_to_csv(fp, write_header=True)

if __name__ == '__main__':
    main()
