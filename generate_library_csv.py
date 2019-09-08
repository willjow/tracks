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

FFPROBE_CMD = ["ffprobe", "-loglevel", "quiet", "-show_entries",
               "format_tags:format", "-of", "json"]
DEFAULT_KEYS = ['album_artist', 'album', 'track', 'title', 'artist', 'duration']


def track_data(track, keys=None):
    """Returns a dict of the track's metadata and stream data.

    Parameters
    ----------
    track : str
        The filename of the track from which to read metadata.
    keys : list, optional
        A list of tags to capture from the track metadata.

    Raises
    ------
    ValueError
        If the ffprobe command fails on the input (track).
    """
    if keys is None:
        keys = DEFAULT_KEYS

    cp = sp.run(FFPROBE_CMD + [track], capture_output=True)

    if cp.returncode:
        raise KeyError(f"Can't read stream/tag data from input ({track})")

    data = json.loads(cp.stdout.decode())['format']
    tags = data['tags']
    del data['tags']
    data.update(tags)
    data = {k: data.get(k) for k in keys}
    return data


def library_data(music_dir, keys=None):
    """Returns a list of dicts containing all the data for tracks in
    music_dir (recursive).

    Parameters
    ----------
    music_dir : str
        The directory in which to search for music files.
    keys : list, optional
        A list of track tags to capture. Defers default to that of track_data().
    """
    errors = []
    tracks = []

    for fn in glob.iglob(music_dir + '**/*', recursive=True):
        try:
            tracks.append(track_data(fn, keys))
        except KeyError:
            if not os.path.isdir(fn):
                sys.stderr.write(f"Can't get track data from {fn}\n")
                errors.append(fn)

    if errors:
        with open('track_data_errors.txt', 'w+') as errorfile:
            for error in errors:
                errorfile.write(f"{error}\n")

    return tracks


def new_tracks(new_df, old_df, keys=DEFAULT_KEYS):
    """Returns the portion (rows) of new_df that includes tracks which aren't
    in old_df.

    Parameters
    ----------
    new_df, old_df : pandas.DataFrame
        The new and old dataframes (libraries), respectively.
    keys : list, optional
        A list of columns to join on. Defaults to DEFAULT_KEYS.
    """
    nt = (pd.merge(new_df, old_df, how='left', on=keys, indicator=True,
                   suffixes=('_new', '_old'))
            .query("_merge == 'left_only'")
            .drop(['_merge', 'catalog_date_old'], axis=1))
    nt.rename(columns={'catalog_date_new': 'catalog_date'}, inplace=True)
    return nt


def update_library_df(music_dir, lib_csv= "lib.csv", keys=DEFAULT_KEYS):
    """Writes a pandas dataframe of the current library

    Parameters
    ----------
    music_dir : str
        The directory in which to search for music files.
    lib_csv : str, optional
        The filepath of the library dataframe csv.
        Defaults to "library.csv"
    keys : list, optional
        A list of tag keys to use for populating the dataframe. This list will
        also be used to determine the order of the columns in the dataframe.
        Defaults to DEFAULT_KEYS.
    """
    # read library to populate dataframe
    new_df = pd.DataFrame.from_dict(library_data(music_dir, keys))
    new_df.fillna('', inplace=True)
    new_df.where(new_df.notnull(), new_df.astype(str), inplace=True)

    # track the date added to the library
    new_date = str(datetime.date.today())
    new_df['catalog_date'] = new_date

    # update existing csv
    if os.path.isfile(lib_csv):
        # preserve the old dates by appending only the new tracks for this call
        old_df = pd.read_csv(lib_csv, dtype=str, na_filter=False)
        new_df = old_df.append(new_tracks(new_df, old_df, keys), sort=False)
        new_df.reset_index(drop=True, inplace=True)

    # reorder columns
    col_order = pd.Index([k for k in keys if k in new_df.columns])
    missing_cols = new_df.columns.difference(col_order, sort=False)
    col_order = col_order.append(missing_cols)

    new_df = new_df[col_order]
    new_df.to_csv(lib_csv, index=False)


if __name__  == '__main__':
    if len(sys.argv) >= 3:
        # convert the keys arg into a list
        sys.argv[2] = ast.literal_eval(sys.argv[2])
    update_library_df(*sys.argv[1:])

