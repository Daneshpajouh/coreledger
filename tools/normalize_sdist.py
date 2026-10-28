"""Rewrite a built sdist so that two builds of one commit give one file.

The build stamps the clock on generated files and the builder's user and
group on every member. This keeps every member's bytes and mode, sorts the
members by name, sets each time to the time of the last commit that changed
a packaged file, clears the owner fields and writes the gzip header with no
name and time 0. Commits that touch only recipe/ or tools/ keep the hash.

    python tools/normalize_sdist.py dist/coreledger-X.tar.gz \
        "$(git log -1 --format=%ct -- src tests pyproject.toml README.md LICENSE)"
"""

from __future__ import annotations

import gzip
import io
import sys
import tarfile


def normalize(path: str, mtime: int) -> None:
    with tarfile.open(path, "r:gz") as src:
        members = sorted(src.getmembers(), key=lambda m: m.name)
        data = {m.name: src.extractfile(m).read() for m in members if m.isfile()}
    buf = io.BytesIO()
    with tarfile.open(fileobj=buf, mode="w", format=tarfile.PAX_FORMAT) as out:
        for m in members:
            m.mtime = mtime
            m.uid = m.gid = 0
            m.uname = m.gname = ""
            m.pax_headers = {}
            out.addfile(m, io.BytesIO(data[m.name]) if m.isfile() else None)
    with open(path, "wb") as fh:
        with gzip.GzipFile(filename="", mode="wb", fileobj=fh, mtime=0) as gz:
            gz.write(buf.getvalue())


if __name__ == "__main__":
    if len(sys.argv) != 3:
        sys.exit("usage: normalize_sdist.py SDIST COMMIT_TIME")
    normalize(sys.argv[1], int(sys.argv[2]))
