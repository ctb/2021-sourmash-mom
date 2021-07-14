# sourmash manifests-of-manifests

This is an early stage dev repo to explore betters ways of doing
database building, by using manifests of manifests and picklists.
See [sourmash#1652](https://github.com/sourmash-bio/sourmash/issues/1652).

You'll need to have the `latest` branch of sourmash
installed (or sourmash 4.2.1, once it's released).

## Quickstart

### Create manifests of manifests

First, create a manifest-of-manifests ('mom') from a directory with zip files:
```
./mom-create.py ./wort.zips/ -o wort-test.db
```
This mimics the situation where you have some really big zip files containing
signatures calculated by e.g. wort.  As output, `wort.db` will contain the
mom.

Second, create another mom DB 
```
./mom-create.py ./tessa.sigs/ -o tessa.db
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
./mom-extract-sigs.py --picklist idents.csv:ident:identprefix \
       wort-test.db tessa.db -k 31 -o save.zip
```

This loads all the moms, does rapid and efficient selection on them using
the picklists, and then loads _just_ the data necessary to give you the
signatures that match. And (of course :) you can save 'em however you want;
here, it's a zip file with a manifest, but it uses the
[standard sourmash output argument style](https://sourmash.readthedocs.io/en/latest/command-line.html#saving-signatures-more-generally).

(Note that moltype and ksize selectors work as expected, but are not
necessary.)

## TODO

Implement ways to update MoMs, so that we don't need to completely
regenerate the database each time for large collections. For really
large collections, this could be split into "update chunked zip files"
(...slow?)  and then "update from directory containing chunks" which
would be fast.
