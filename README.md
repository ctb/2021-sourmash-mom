# sourmash manifests-of-manifests

This is an early stage dev repo to explore betters ways of doing
database building, by using manifests of manifests and picklists.
See [sourmash#1652](https://github.com/sourmash-bio/sourmash/issues/1652).

You'll need to have the `add/manifest_lazy_sigfile` branch of sourmash
installed
([PR sourmash#1619](https://github.com/sourmash-bio/sourmash/pull/1619))
for the experimental code implementing (1) `class ManifestOfManifests`
and (2) `LazyLoadedIndex`.

## Quickstart

### Create manifests of manifests

First, create a manifest-of-manifests ('mom') from a directory with zip files:
```
./create-sqlite3-mom.py wort.db ./wort.zips/
```
This mimics the situation where you have some really big zip files containing
signatures calculated by e.g. wort.  As output, `wort.db` will contain the
mom.

Second, create another mom DB 
```
./create-sqlite3-mom.py tessa.db ./tessa.sigs/
```
from a directory containing a pile of signatures. This mimics the
situation where there are a bunch of ancillary signatures that are
needed because they're not in the big collection.

Now you have two .db files that are manifests of manifests, containing
both index locations *and* internal locations of signatures.

### Extract matching sigs

Now, use the picklist of identifiers in `idents.csv` to extract the
sigs you care about:

```
./mom-extract-sigs.py --picklist idents.csv:ident:identprefix *.db \
       -k 31 -o save.zip
```

This loads all the moms, does rapid and efficient selection on them using
the picklists, and then loads _just_ the data necessary to give you the
signatures that match. And (of course :) you can save 'em however you want;
here, it's a zip file with a manifest, but it uses the
[standard sourmash output argument style](https://sourmash.readthedocs.io/en/latest/command-line.html#saving-signatures-more-generally).

(Note that moltype and ksize selectors work as expected, but are not
necessary.)

## TODO

Do we want to abspath the locations of the indexes? We should at least
provide the option on creation.

Also, provide default dedup on md5sum?

Implement ways to update MoMs, so that we don't need to completely
regenerate the database each time for large collections. For really
large collections, this could be split into "update chunked zip files"
(...slow?)  and then "update from directory containing chunks" which
would be fast.
