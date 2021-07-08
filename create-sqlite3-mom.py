#! /usr/bin/env python
"""
Create a sqlite3 database that contains a manifest-of-manifests.
"""
import sys
import sqlite3
import argparse
import sourmash
from sourmash.manifest import ManifestOfManifests


def main():
    p = argparse.ArgumentParser()
    p.add_argument('sqlite_db')
    p.add_argument('--from-file')
    p.add_argument('sigs_or_dbs', nargs='*')
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

    sigs_or_dbs = list(args.sigs_or_dbs)
    if args.from_file:
        with open(args.from_file) as fp:
            lines = [ x.strip() for x in fp ]
            sigs_or_dbs.extend(lines)

    if not sigs_or_dbs:
        print('ERROR: no sigs or dbs?')
        sys.exit(-1)

    index_locations = []
    manifests = []
    for location in sigs_or_dbs:
        idx = sourmash.load_file_as_index(location)
        assert idx.manifest, (location, idx)
        index_locations.append(location)
        manifests.append(idx.manifest)

    print(f'loaded {len(index_locations)} index locations')
    mom = ManifestOfManifests(index_locations, manifests)
    print(f'len {len(mom)} signatures total')

    for l, m in mom.index_locations_and_manifests():
        for row in m.rows:
            cursor.execute('INSERT INTO manifest (index_location, internal_location, md5, md5short, ksize, moltype, num, scaled, n_hashes, with_abundance, name, filename) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)',
                           (l, row['internal_location'], row['md5'], row['md5short'], row['ksize'], row['moltype'], row['num'], row['scaled'], row['n_hashes'], row['with_abundance'], row['name'], row['filename']),)

        db.commit()

    cursor.execute('SELECT COUNT(DISTINCT md5) from manifest')
    count, = cursor.fetchone()
    print(f'{count} distinct rows total')

if __name__ == '__main__':
    sys.exit(main())
