"""
update a sqlite3 database that contains a manifest-of-manifests built from
a list of directories.

detects changed files, as well as new files, using modification time.

might be slow on really large directories...

CTB: add mtime to manifest-of-manifests
CTB: is mtime something that Storage should support?
"""
import sys
import os
import pathlib
import sqlite3
import argparse
import sourmash
from sourmash.manifest import ManifestOfManifests
from sourmash.index import DirectoryIndex


def main():
    p = argparse.ArgumentParser()
    p.add_argument('sqlite_db')
    p.add_argument('--from-file')
    p.add_argument('dirlist', nargs='*')
    args = p.parse_args()
    db = sqlite3.connect(args.sqlite_db)

    # load manifest entries from a SQLite database
    m = ManifestOfManifests.load_from_sqlite(args.sqlite_db)
    print(f"{len(m)} signature entries total in database.")

    
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

    db_path = pathlib.Path(args.sqlite_db)
    db_mtime = db_path.stat().st_mtime

    # find files to update
    update_files = []
    cur_files = set()
    for l, _ in m.index_locations_and_manifests():
        cur_files.add(l)

        p = pathlib.Path(l)
        if p.stat().st_mtime > db_mtime:
            print(f"'{p}' is recently modified; updating")
            idx = sourmash.load_file_as_index(l)
            assert not isinstance(idx, DirectoryIndex), l
            m.update_manifest(l, idx.manifest)
            update_files.append(str(p))

    # find files to add
    new_files = set([ str(x) for x in file_list ]) - cur_files
    print('new files:', new_files)

    for l in new_files:
        idx = sourmash.load_file_as_index(l)
        m.update_manifest(idx, idx.manifest)
        print(f"added '{l}'; {len(idx.manifest)} entries in manifest)")

    sys.exit(0)

    # load each filename individually as an index
    index_locations = []
    manifests = []
    for path in file_list:
        filename = str(path)
        idx = sourmash.load_file_as_index(filename)
        assert idx.manifest, (filename, idx)
        index_locations.append(filename)
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

