#! /usr/bin/env python
import sys
import argparse
import time
import csv

from sourmash import sourmash_args
from sourmash.index import LazyLoadedIndex
from sourmash.cli.utils import (add_ksize_arg, add_moltype_args,
                                add_picklist_args)

from libmom import ManifestOfManifests


def main():
    p = argparse.ArgumentParser()
    p.add_argument('dblist', nargs='+')
    p.add_argument('-o', '--output', default=None)
    p.add_argument('--save-unmatched-pickset')
    add_ksize_arg(p, 31)
    add_moltype_args(p)
    add_picklist_args(p)
    args = p.parse_args()

    ksize = args.ksize
    moltype = sourmash_args.calculate_moltype(args)
    if not (args.ksize or moltype):
        print("NOTE: no ksize/moltype selector given. Are you sure?")

    picklist = sourmash_args.load_picklist(args)

    # load one or more manifests of manifests, and select on them by picklist
    moms = []
    sum_matches = 0
    for db in args.dblist:
        print(f"Loading MoM sqlite database {db}...")
        nrows = ManifestOfManifests.nrows(db)
        print(f"{db} contains {nrows} rows total. Running select......")
        start_time = time.time()
        mom = ManifestOfManifests.load_from_sqlite(db, ksize=ksize,
                                                   moltype=moltype,
                                                   picklist=picklist)
        end_time = time.time()
        diff_time = end_time - start_time
        print(f"...{len(mom)} matches remaining for '{db}' ({diff_time:.1f}s)")

        sum_matches += len(mom)
        moms.append(mom)

    print("---")

    print(f"loaded {sum_matches} rows total from {len(moms)} databases.")

    # report on picklist matches - this is where things would exit
    # if --picklist-require-all was set.
    if picklist and args.save_unmatched_pickset:
        unfound = picklist.pickset - picklist.found

        with open(args.save_unmatched_pickset, "w", newline="") as fp:
            w = csv.writer(fp)
            w.writerow(["unmatched"])
            for v in unfound:
                w.writerow([v])
        print(f"Wrote {len(unfound)} unmatched values from picklist to '{args.save_unmatched_pickset}'", file=sys.stderr)

    sourmash_args.report_picklist(args, picklist)

    # CTB
    distinct = set()
    for mom in moms:
        for idx_location, manifest in mom.index_locations_and_manifests():
            for row in manifest.rows:
                tup = (row['name'], row['md5'])
                distinct.add(tup)
    print(f"There are {len(distinct)} distinct rows across all MoMs.")
    num_distinct = len(distinct)

    if not args.output:
        print('No output options; exiting.', file=sys.stderr)
        sys.exit(0)

    print("---")
    print(f"Now extracting {num_distinct} signatures to '{args.output}'")

    # save sigs to args.output -
    save_sigs = sourmash_args.SaveSignaturesToLocation(args.output)
    save_sigs.open()

    n = 0
    # for every manifest-of-manifest,

    already_saved = set()
    for mom in moms:
        # get the index locations,
        for idx_location, manifest in mom.index_locations_and_manifests():
            idx = LazyLoadedIndex(idx_location, manifest)

            # and pull out all signatures in the manifest,
            for ss in idx.signatures():
                # check it matches our identifier list and is not a dup:
                tup = (ss.name, ss.md5sum())
                if ss in picklist and tup not in already_saved:
                    # and save!
                    save_sigs.add(ss)
                    already_saved.add(tup)
                    n += 1

                if n % 10 == 0:
                    print(f'\33[2K...{n} signatures of {num_distinct} saved.', end="\r")

    print(f'...{n} signatures of {num_distinct} saved.')

if __name__ == '__main__':
    main()
