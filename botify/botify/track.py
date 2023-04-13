import itertools
import json
import pickle
from dataclasses import dataclass, field
from typing import List


@dataclass
class Track:
    track: int
    artist: str
    title: str
    recommendations: List[int] = field(default=lambda: [])


@dataclass
class UserPreferences:
    user: int
    preferences: List[int] = field(default=lambda: [])


class Catalog:
    """
    A helper class used to load track data upon server startup
    and store the data to redis.
    """

    def __init__(self, app):
        self.app = app
        self.tracks = []
        self.top_tracks = []
        self.hw_tracks = []
        self.users_preferences = []

    def load(self, catalog_path, top_tracks_path, hw_catalog_path, users_preferences_path):
        self.app.logger.info(f"Loading tracks from {catalog_path}")
        with open(catalog_path) as catalog_file:
            for j, line in enumerate(catalog_file):
                data = json.loads(line)
                self.tracks.append(
                    Track(
                        data["track"],
                        data["artist"],
                        data["title"],
                        data.get("recommendations", []),
                    )
                )
        self.app.logger.info(f"Loaded {j+1} tracks")

        self.app.logger.info(f"Loading tracks from {hw_catalog_path}")
        with open(hw_catalog_path) as catalog_file:
            for j, line in enumerate(catalog_file):
                data = json.loads(line)
                self.hw_tracks.append(
                    Track(
                        data["track"],
                        data["artist"],
                        data["title"],
                        data.get("recommendations", []),
                    )
                )
        self.app.logger.info(f"Loaded {j + 1} tracks")

        self.app.logger.info(f"Loading top tracks from {top_tracks_path}")
        with open(top_tracks_path) as top_tracks_path_file:
            self.top_tracks = json.load(top_tracks_path_file)
        self.app.logger.info(f"Loaded top tracks {self.top_tracks[:3]} ...")

        self.app.logger.info(f"Loading users preferences from {users_preferences_path}")
        with open(users_preferences_path) as catalog_file:
            for j, line in enumerate(catalog_file):
                data = json.loads(line)
                self.users_preferences.append(
                    UserPreferences(
                        data["user"],
                        data.get("preferences", []),
                    )
                )
        self.app.logger.info(f"Loaded {j + 1} users preferences")

        return self

    def upload_tracks(self, redis, tracks):
        self.app.logger.info(f"Uploading tracks to redis")
        for track in tracks:
            redis.set(track.track, self.to_bytes(track))
        self.app.logger.info(f"Uploaded {len(tracks)} tracks")

    def upload_users_preferences(self, redis):
        self.app.logger.info(f"Uploading users preferences to redis")
        for user_preferences in self.users_preferences:
            redis.set(user_preferences.user, self.to_bytes(user_preferences))
        self.app.logger.info(f"Uploaded {len(self.users_preferences)} users preferences")

    def upload_artists(self, redis):
        self.app.logger.info(f"Uploading artists to redis")
        sorted_tracks = sorted(self.tracks, key=lambda track: track.artist)
        for j, (artist, artist_catalog) in enumerate(
            itertools.groupby(sorted_tracks, key=lambda track: track.artist)
        ):
            artist_tracks = [t.track for t in artist_catalog]
            redis.set(artist, self.to_bytes(artist_tracks))
        self.app.logger.info(f"Uploaded {j + 1} artists")

    def to_bytes(self, instance):
        return pickle.dumps(instance)

    def from_bytes(self, bts):
        return pickle.loads(bts)