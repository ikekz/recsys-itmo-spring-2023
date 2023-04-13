from .random import Random
from .recommender import Recommender
import random


class Homework(Recommender):
    def __init__(self, tracks_redis, catalog, users_preferences):
        self.tracks_redis = tracks_redis
        self.users_preferences = users_preferences
        self.fallback = Random(tracks_redis)
        self.catalog = catalog

    def recommend_next(self, user: int, prev_track: int, prev_track_time: float) -> int:
        previous_track = self.tracks_redis.get(prev_track)
        user_preferences = self.users_preferences.get(user)

        if previous_track is None or user_preferences is None:
            return self.fallback.recommend_next(user, prev_track, prev_track_time)

        previous_track = self.catalog.from_bytes(previous_track)
        recommendations = previous_track.recommendations

        user_preferences = self.catalog.from_bytes(user_preferences)
        preferences = user_preferences.preferences

        if not recommendations:
            return self.fallback.recommend_next(user, prev_track, prev_track_time)

        result_recommendations = set(recommendations) & set(preferences)
        if len(result_recommendations) > 0:
            recommendations = result_recommendations

        shuffled = list(recommendations)
        random.shuffle(shuffled)
        return shuffled[0]
