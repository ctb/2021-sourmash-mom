#! /usr/bin/env python
import sys
import argparse

import sourmash
from sourmash import sourmash_args
from sourmash.manifest import ManifestOfManifests
from sourmash.index import LazyMultiIndex, LazyLoadedIndex
from sourmash.cli.utils import (add_ksize_arg, add_moltype_args,
                                add_picklist_args)


def main():
    p = argparse.ArgumentParser()
    p.add_argument('dblist', nargs='+')
    p.add_argument('-o', '--output', default=None)
    add_ksize_arg(p, 31)
    add_moltype_args(p)
    add_picklist_args(p)
    args = p.parse_args()

    ksize = args.ksize
    moltype = sourmash_args.calculate_moltype(args)
    picklist = sourmash_args.load_picklist(args)

    # load one or more manifests of manifests, and select on them by picklist
    moms = []
    sum_matches = 0
    for db in args.dblist:
        mom = ManifestOfManifests.load_from_sqlite(db)
        mom = mom.select_to_manifest(ksize=ksize, moltype=moltype,
                                     picklist=picklist)
        sum_matches += len(mom)
        moms.append(mom)

    # report on picklist matches - this is where things would exit
    # if --picklist-require-all was set.
    sourmash_args.report_picklist(args, picklist)

    if not args.output:
        print('No output options; exiting.', file=sys.stderr)
        sys.exit(0)

    # save sigs to args.output -
    save_sigs = sourmash_args.SaveSignaturesToLocation(args.output)
    save_sigs.open()

    n = 0
    # for every manifest-of-manifest,
    for mom in moms:
        # get the index locations,
        for idx_location, manifest in mom.index_locations_and_manifests():
            idx = LazyLoadedIndex(idx_location, manifest)

            # and pull out all signatures in the manifest,
            for ss in idx.signatures():
                # and save!
                assert ss in picklist
                save_sigs.add(ss)
                n += 1

                if n % 10 == 0:
                    print(f'...{n} signatures of {sum_matches} saved.')

    print(f'...{n} signatures of {sum_matches} saved.')

if __name__ == '__main__':
    main()
