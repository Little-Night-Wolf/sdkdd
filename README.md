A Python script designed to migrate filesystem and database information used by an existing Kemono/OpenYiff instance created before *September 26, 2021* to a new SHA256-based hash folder structure with zero data loss, as well as perform long-needed cleanup and maintenance tasks.

## Running
Still not complete. I wouldn't run this outside of dry-mode yet.

```bash
# libmagic is required. make sure it is installed (https://github.com/ahupp/python-magic#installation)
# make sure you are on the latest kitsune version before doing anything
# although running sdkdd involves a 0% chance of losing data, it would still be extremely wise to back up
virtualenv venv
source venv/bin/activate # source venv/bin/activate.fish if you're a cool kid
pip install -r requirements.txt
mv config.py.example config.py # configure
python3 sdkdd.py
```

`sdkdd` will begin moving files and changing database entries. When it is done, everything left in `files`, `attachments`, and `inline` are duplicate/garbage files that can be safely discarded.

## TODO
- [x] Finish file-tracking migrations in Kitsune (table creation)
- [x] Finish actual downloading behavior in Kitsune (sha256 filenames, skip move operation on existence, replace .jpe with .jpg)
- [ ] Change Kemono render behavior (don't render images with the same path)
- [x] Check magic number for file type and change extensions
- [x] Change ctime/mtime handling to use datetimes instead of floats
### Low priority
- [ ] Add rollback/revert mode that runs on previous migration logs
