"SQLite-based manifest-of-manifest class for sourmash."

from collections import defaultdict
from sourmash.manifest import CollectionManifest

class ManifestOfManifests:
    # @CTB rename to MultiManifest?
    def __init__(self, index_locations, manifests):
        assert len(index_locations) == len(manifests)
        self.index_locations = index_locations
        self.manifests = manifests

    def __len__(self):
        return sum([ len(m) for m in self.manifests ])

    @classmethod
    def nrows(cls, filename):
        "Count total number of rows in this database, with no filtering."
        import sqlite3
        db = sqlite3.connect(filename)
        cursor = db.cursor()
        query = 'SELECT COUNT(md5) FROM manifest'
        cursor.execute(query)
        total_rows, = cursor.fetchone()
        return total_rows

    @classmethod
    def load_from_sqlite(cls, filename, *,
                         ksize=None, moltype=None, picklist=None):
        import sqlite3
        db = sqlite3.connect(filename)
        cursor = db.cursor()

        query = 'SELECT DISTINCT index_location, internal_location, md5, md5short, ksize, moltype, num, scaled, n_hashes, with_abundance, name, filename FROM manifest'
        conditions = []
        args = []
        if ksize:
            conditions.append('ksize=?')
            args.append(int(ksize))
        if moltype:
            conditions.append('moltype=?')
            args.append(moltype)

        if conditions:
            query += ' WHERE ' + " AND ".join(conditions)

        cursor.execute(query, args)
        rowkeys = 'internal_location, md5, md5short, ksize, moltype, num, scaled, n_hashes, with_abundance, name, filename'.split(', ')

        d = defaultdict(list)
        #print(rowkeys)
        for result in cursor:
            loc, *rest = result
            mrow = dict(zip(rowkeys, rest))
            if picklist and not picklist.matches_manifest_row(mrow):
                continue
            d[loc].append(mrow)

        locs = []
        manifests = []
        for loc, value in d.items():
            manifest = CollectionManifest(value)
            locs.append(loc)
            manifests.append(manifest)

        return cls(locs, manifests)

    def select_to_manifest(self, **kwargs):
        new_manifests = []
        for m in self.manifests:
            m = m.select_to_manifest(**kwargs)
            new_manifests.append(m)
        return ManifestOfManifests(self.index_locations, new_manifests)

    def update_manifest(self, index_location, new_manifest):
        idx_locations = self.index_locations
        # CTB: note, this assumes idx_locations has no duplicates
        for i in range(len(idx_locations)):
            if idx_locations[i] == index_location:
                # found! update & exit
                self.manifests[i] = new_manifest
                return

        # not found?
        self.index_locations.append(index_location)
        self.manifests.append(new_manifest)

    def locations(self):
        raise NotImplementedError

    def index_locations_and_manifests(self):
        for (l, m) in zip(self.index_locations, self.manifests):
            yield l, m

    def __contains__(self, ss):
        for m in self.manifests:
            if ss in m:
                return True
        return False
