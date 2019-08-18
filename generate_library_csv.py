#!/usr/bin/env python
import datetime
import json
import glob
import os
import pandas as pd
import pickle
import subprocess as sp
import sys

FFPROBE_CMD = ["ffprobe", "-loglevel", "quiet", "-show_entries",
               "format_tags:format", "-of", "json"]
DEFAULT_KEYS = ['album_artist', 'album', 'track', 'title', 'artist', 'duration']

TEST_FILE = "/home/wjow/music/dl/Madlib/Jackson Conti/Jackson Conti - Sujinho/01 - Mamaoism.mp3"
TEST_BAD_FILE = "/home/wjow/music/dl/Madlib/Jackson Conti/cover.png"
TEST_DIR = "/home/wjow/music/dl/Madlib/MED, Blu & Madlib/"


def track_data(track):
    """Returns a dict of the track's metadata and stream data.

    Parameters
    ----------
    track : str
        The filename of the track from which to read metadata.

    Raises
    ------
    ValueError
        If the ffprobe command fails on the input (track).
    """
    cp = sp.run(FFPROBE_CMD + [track], capture_output=True)

    if cp.returncode:
        raise ValueError(f"Can't read stream/tag data from input ({track})")

    data = json.loads(cp.stdout.decode())['format']
    tags = data['tags']
    del data['tags']
    data.update(tags)
    return data


def current_library(music_dir):
    """Returns a list of dicts containing all the data for tracks in
    music_dir (recursive).

    Parameters
    ----------
    music_dir : str
        The directory in which to search for music files.
    """
    errors = []
    tracks = []

    for fn in glob.iglob(music_dir + '**/*', recursive=True):
        try:
            tracks.append(track_data(fn))
        except ValueError:
            if not os.path.isdir(fn):
                sys.stderr.write(f"Can't get track data from {fn}\n")
                errors.append(fn)

    with open('track_data_errors.txt', 'w') as errorfile:
        for error in errors:
            errorfile.write(f"{error}\n")

    return tracks


def update_library_df(music_dir, lib_csv=None):
    """Serializes a pandas dataframe of the current library

    Parameters
    ----------
    music_dir : str
        The directory in which to search for music files.
    lib_csv : str, optional
        The filepath of the library dataframe csv.
        Defaults to "library.csv"
    """
    if lib_csv is None:
        lib_csv = "lib_csv.csv"

    new_df = pd.DataFrame(current_library(music_dir))
    new_date = str(datetime.date.today())
    new_df['date_added_to_library'] = new_date

    if os.path.isfile(lib_csv):
        old_df = pd.read_csv(lib_csv)
        new_tracks = (pd.merge(new_df, old_df, how='left', on='title',
                               indicator=True)
                        .query("_merge == 'left_only'")
                        .drop('_merge', axis=1)
        new_df = old_df.append(new_tracks)

    new_df.to_csv(lib_csv)

if __name__  == '__main__':
    update_library_df(sys.argv[1])

