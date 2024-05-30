#!/usr/bin/env python
import ast
import datetime
import json
import glob
import os
import pickle
import subprocess as sp
import sys

import pandas as pd

FFPROBE_CMD = [
    "ffprobe",
    "-loglevel",
    "quiet",
    "-show_streams",
    "-select_streams",
    "a",
    "-show_entries",
    "format",
    "-output_format",
    "json",
    "--",
]
DEFAULT_KEYS = [
    "album_artist",
    "album",
    "track",
    "title",
    "artist",
    "duration",
    "genre",
]


def track_data(track, keys=None):
    """Return a dict of the track's metadata and stream data.

    Parameters
    ----------
    track : str
        The filename of the track from which to read metadata.
    keys : list, optional
        A list of tags to capture from the track metadata.

    Raises
    ------
    CalledProcessError
        If the ffprobe command fails on the input (track).
    ValueError
        If the track does not contain any audio streams.
    """
    if keys is None:
        keys = DEFAULT_KEYS

    ffprobe = sp.run(FFPROBE_CMD + [track], capture_output=True)
    ffprobe.check_returncode()

    data = json.loads(ffprobe.stdout.decode())
    if not data["streams"]:
        raise ValueError(f"{track} contains no audio streams")

    format_tags = data["format"]
    tags = format_tags["tags"]
    del format_tags["tags"]
    format_tags.update(tags)
    format_tags = {k: format_tags.get(k) for k in keys}
    return format_tags


def library_data(music_dir, keys=None):
    """Return a generator of dicts containing all the data for tracks in
    music_dir (recursive).

    Parameters
    ----------
    music_dir : str
        The directory in which to search for music files.
    keys : list, optional
        A list of track tags to capture. Defers default to that of track_data().
    """
    with open("track_data_errors.txt", "w+") as errorfile:
        for fn in glob.iglob(music_dir + "**/*", recursive=True):
            try:
                yield track_data(fn, keys)
            except KeyError:
                if not os.path.isdir(fn):
                    sys.stderr.write(f"Can't get track data from {fn}\n")
                    errorfile.write(f"{error}\n")


def update_db(music_dir, db="lib.sqlite3", keys=DEFAULT_KEYS):
    for td in library_data(music_dir, keys):
        pass


if __name__ == "__main__":
    if len(sys.argv) >= 3:
        # convert the keys arg into a list
        sys.argv[2] = ast.literal_eval(sys.argv[2])
    update_db(*sys.argv[1:])
