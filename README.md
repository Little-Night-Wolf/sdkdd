A collection of Python scripts designed to migrate filesystem and database information used by an existing Kemono/OpenYiff instance created before *September 28, 2021* to a new SHA256-based hash folder structure (`/ha/sh/hashpath`) with zero data loss, as well as perform long-needed cleanup and maintenance tasks. Running it is not required to maintain regular operation of __Kitsune__ (archiving jobs themselves will not fail or have issues,) but it is highly recommended if you run __Kemono 2__ (the frontend,) as it will slowly assume the new data structures are being used over time; you may find images and files not rendering correctly in a few months' time if you continue to update your instance without fully bringing it up to speed.

Again, if your instance was created after *September 28, 2021*, you do not need to run this. Pretend you never saw this repo.

## Running
There's no built-in revert mode yet due to time constraints; one can create one themselves by reading the contents of the `sdkdd_migration_<epoch time>` log table, but generally, if you aren't 100% committed to moving, don't proceed.

```bash
# libmagic is required. make sure it is installed (https://github.com/ahupp/python-magic#installation)
# make sure you are on the latest kitsune version before doing anything
# although running sdkdd involves a 0% chance of losing data, it would still be extremely wise to back up your db
virtualenv venv
source venv/bin/activate # source venv/bin/activate.fish if you're a cool kid
pip install -r requirements.txt
mv config.py.example config.py # configure. your postgres ports and data folders need to be exposed/mounted to the host respectively.
python3 sdkdd.py
```

`sdkdd` will begin moving files and changing database entries. A log of all operations will be output to a table with the name `sdkdd_migration_<epoch time>`. When it is done, everything left in `files`, `attachments`, and `inline` are duplicate/garbage files that can be safely discarded.

## FAQ
### I stopped sdkdd in the middle of a wet run! Is running it again fine?
Yes. Just re-run the script, and it will pick up where it left off.
### Can I run this live?
Yes. `sdkdd` can run while your instance is on, provided you are on the latest Kitsune version to avoid potential race conditions.
## TODO
- [x] Finish file-tracking migrations in Kitsune (table creation)
- [x] Finish actual downloading behavior in Kitsune (sha256 filenames, skip move operation on existence, replace .jpe with .jpg)
- [ ] Change Kemono render behavior (don't render images with the same path) (will be done after party's migration finishes)
- [x] Check magic number for file type and change extensions
- [x] Change ctime/mtime handling to use datetimes instead of floats
### Low priority
- [ ] Add rollback/revert mode that runs on previous migration logs
