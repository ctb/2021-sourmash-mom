#! /usr/bin/env python
"""
Create a sqlite3 database that contains a manifest-of-manifests.
"""
import sys
import os
import pathlib
import sqlite3
import argparse
import sourmash
from sourmash.manifest import ManifestOfManifests
import time


def main():
    p = argparse.ArgumentParser()
    p.add_argument('sqlite_db')
    p.add_argument('--from-file')
    p.add_argument('dirlist', nargs='*')
    p.add_argument('--abspath', action='store_true')
    args = p.parse_args()
    db = sqlite3.connect(args.sqlite_db)

    cursor = db.cursor()
    try:
        cursor.execute("""

    CREATE TABLE manifest (
        index_location TEXT,
        internal_location TEXT,
        md5 TEXT,
        md5short TEXT,
        ksize INTEGER,
        moltype TEXT,
        num INTEGER,
        scaled INTEGER,
        n_hashes INTEGER,
        with_abundance INTEGER,
        name TEXT,
        filename TEXT
    )

    """)
        print('created table')
    except sqlite3.OperationalError:
        # already exists?
        print('table exists! not creating.')

    dirlist = list(args.dirlist)
    if args.from_file:
        with open(args.from_file) as fp:
            lines = [ x.strip() for x in fp ]
            dirlist.extend(lines)

    if not dirlist:
        print('ERROR: no dirlist?')
        sys.exit(-1)

    for filename in dirlist:
        if not os.path.isdir(filename):
            print(f"ERROR: '{filename}' is not a directory.")
            sys.exit(-1)

    # pull in all .sig, .sig.gz, and .zip filenames
    file_list = []
    for dirname in dirlist:
        p = pathlib.Path(dirname)
        file_list.extend(p.glob('**/*.sig'))
        file_list.extend(p.glob('**/*.sig.gz'))
        file_list.extend(p.glob('**/*.zip'))

    # load each filename individually as an index
    index_locations = []
    manifests = []
    
    for path in file_list:
        print('loading from:', path, end='\r')
        filename = str(path)
        if args.abspath:
            filename = str(path.resolve())
        start_time = time.time()
        try:
            idx = sourmash.load_file_as_index(filename)
        except:
            print(f"ERROR loading from {filename}; skipping.")
            continue

        end_time = time.time()

        if not idx.manifest:
            print(f"ERROR loading from {filename}; no manifest. Exiting.")
            sys.exit(-1)
        else:
            diff_time = end_time - start_time
            print(f"Loaded {len(idx.manifest)} signatures from {filename} ({diff_time:.2f}s)")
        
        index_locations.append(filename)
        manifests.append(idx.manifest)

    print("")
    print(f'Finished loading{len(index_locations)} index locations.')
    mom = ManifestOfManifests(index_locations, manifests)
    print(f'Got {len(mom)} signatures.')

    for l, m in mom.index_locations_and_manifests():
        for row in m.rows:
            cursor.execute('INSERT INTO manifest (index_location, internal_location, md5, md5short, ksize, moltype, num, scaled, n_hashes, with_abundance, name, filename) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)',
                           (l, row['internal_location'], row['md5'], row['md5short'], row['ksize'], row['moltype'], row['num'], row['scaled'], row['n_hashes'], row['with_abundance'], row['name'], row['filename']),)

        db.commit()

    cursor.execute('SELECT COUNT(DISTINCT md5) from manifest')
    count, = cursor.fetchone()
    print(f'There are now {count} distinct md5 hashes in database')

if __name__ == '__main__':
    sys.exit(main())
