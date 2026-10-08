"""Regression tests for the Stremio "There is no information available" bug.

stremio-core deserializes a `/meta/{type}/{id}.json` response into a strict
Rust struct. Unlike `/catalog` (which uses `VecSkipError` and silently drops
bad entries), the `meta` key has NO error tolerance: one unparseable field
discards the whole response and the details page renders
"There is no information available".

Every test here pins one of the field shapes that used to break that.
"""
import json

import pytest

from .conftest import TOKEN, file_doc, meta_doc
from . import stremio_spec as spec


def get_meta(client, mtype, mid):
    r = client.get(f"/stremio/{TOKEN}/meta/{mtype}/{mid}.json")
    assert r.status_code == 200, r.text
    return r.json()


def get_catalog(client, mtype, cat):
    r = client.get(f"/stremio/{TOKEN}/catalog/{mtype}/global_{cat}.json")
    assert r.status_code == 200, r.text
    return r.json()


def get_streams(client, mtype, mid):
    r = client.get(f"/stremio/{TOKEN}/stream/{mtype}/{mid}.json")
    assert r.status_code == 200, r.text
    return r.json()["streams"]


# ---------------------------------------------------------------------------
# The headline bug: trailerStreams
# ---------------------------------------------------------------------------


async def test_trailer_is_a_real_stream_object(client, seed):
    """`{"source": "<yt key>"}` matches no StreamSource variant, so stremio-core
    rejected the entire meta response for EVERY title that had a trailer."""
    await seed.meta(meta_doc("tmdb:278", imdb_id="tt0111161",
                             aliases=["tmdb:278", "tt0111161"],
                             title="The Shawshank Redemption", year=1994,
                             trailer_yt="NmzuHjWmXOc"))
    body = get_meta(client, "movie", "tt0111161")
    assert spec.check_meta(body, "movie", "tt0111161") == []

    trailers = body["meta"]["trailerStreams"]
    assert trailers == [{"ytId": "NmzuHjWmXOc", "name": "Trailer",
                         "description": "▶ Official Trailer"}]
    assert "source" not in trailers[0]


async def test_trailer_full_youtube_url_is_reduced_to_ytid(client, seed):
    await seed.meta(meta_doc("tmdb:603", imdb_id="tt0133093",
                             aliases=["tmdb:603", "tt0133093"],
                             trailer_yt="https://www.youtube.com/watch?v=m8e-FF8MsqU"))
    trailers = get_meta(client, "movie", "tt0133093")["meta"]["trailerStreams"]
    assert trailers[0]["ytId"] == "m8e-FF8MsqU"


@pytest.mark.parametrize("bad_key", [
    "not-a-youtube-id", "", None, "abc", 12345,
    "https://youtube.com/playlist?list=PLrAXtmErZgOeiKm4sgNOknGvNjby9efdf",
])
async def test_unusable_trailer_key_is_dropped_not_emitted(client, seed, bad_key):
    """A malformed key must never reach the wire — omitting the trailer keeps
    the title working, emitting a bad Stream kills the whole page."""
    await seed.meta(meta_doc("tmdb:999", imdb_id="tt0000999",
                             aliases=["tmdb:999", "tt0000999"],
                             trailer_yt=bad_key))
    body = get_meta(client, "movie", "tt0000999")
    assert spec.check_meta(body, "movie", "tt0000999") == []
    assert "trailerStreams" not in body["meta"]
    assert body["meta"]["name"] == "Some Title"


# ---------------------------------------------------------------------------
# Content type
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("stored,expected", [
    ("tv", "series"), ("TV", "series"), ("show", "series"),
    ("series", "series"), ("movie", "movie"),
])
async def test_media_type_is_normalized_to_a_stremio_type(client, seed, stored, expected):
    """Stremio reads type "tv" as a LIVE channel, so a VOD series stored as
    "tv" (the upstream project's convention) rendered as an empty page."""
    mid = f"tmdb:{abs(hash(stored)) % 99999}"
    catalog = "other_series" if expected == "series" else "other_movies"
    await seed.meta(meta_doc(mid, imdb_id=None, aliases=[mid],
                             media_type=stored, catalog=catalog))
    await seed.files(file_doc(mid, -1001, 1, "Show.S01E01.720p.mkv",
                              season=1, episode_start=1, episode_end=1))
    body = get_meta(client, expected, mid)
    assert body["meta"]["type"] == expected
    assert spec.check_meta(body, expected, mid) == []


@pytest.mark.parametrize("stored", [None, "", "weird", "documentary"])
async def test_unusable_media_type_falls_back_to_the_requested_type(client, seed, stored):
    """The catalog advertises a type; the meta answer must not contradict it."""
    mid = f"tmdb:{abs(hash(str(stored))) % 99999}"
    await seed.meta(meta_doc(mid, imdb_id=None, aliases=[mid],
                             media_type=stored, catalog="other_series"))
    await seed.files(file_doc(mid, -1001, 1, "Show.S01E01.720p.mkv",
                              season=1, episode_start=1, episode_end=1))
    body = get_meta(client, "series", mid)
    assert body["meta"]["type"] == "series"
    assert spec.check_meta(body, "series", mid) == []


async def test_catalog_never_emits_type_tv(client, seed):
    mid = "tmdb:1399"
    await seed.meta(meta_doc(mid, imdb_id="tt0944947",
                             aliases=[mid, "tt0944947"], media_type="tv",
                             catalog="other_series"))
    await seed.files(file_doc(mid, -1002, 1, "GoT.S01E01.mkv",
                              season=1, episode_start=1, episode_end=1))
    await seed.catalogs()
    body = get_catalog(client, "series", "other_series")
    assert body["metas"][0]["type"] == "series"
    assert spec.check_catalog(body) == []


# ---------------------------------------------------------------------------
# Scalar hygiene
# ---------------------------------------------------------------------------


async def test_missing_year_never_renders_the_string_none(client, seed):
    await seed.meta(meta_doc("tmdb:11", imdb_id="tt0000011",
                             aliases=["tmdb:11", "tt0000011"], year=None))
    await seed.catalogs()
    await seed.files(file_doc("tmdb:11", -1001, 11, "No.Year.mkv"))

    meta_body = get_meta(client, "movie", "tt0000011")
    assert "None" not in json.dumps(meta_body)
    assert meta_body["meta"].get("year") in (None, "")
    assert meta_body["meta"]["releaseInfo"] == ""

    cat_body = get_catalog(client, "movie", "other_movies")
    assert cat_body["metas"][0]["releaseInfo"] == ""
    assert spec.check_catalog(cat_body) == []


async def test_running_series_release_info_uses_the_spec_format(client, seed):
    await seed.meta(meta_doc("tmdb:12", imdb_id="tt0000012",
                             aliases=["tmdb:12", "tt0000012"],
                             year=2011, media_type="series",
                             catalog="other_series"))
    assert get_meta(client, "series", "tt0000012")["meta"]["releaseInfo"] == "2011-"


async def test_moviedb_id_is_a_string_and_never_null(client, seed):
    await seed.meta(meta_doc("tmdb:13", imdb_id="tt0000013",
                             aliases=["tmdb:13", "tt0000013"], tmdb_id=13))
    await seed.meta(meta_doc("tmdb:14", imdb_id="tt0000014",
                             aliases=["tmdb:14", "tt0000014"], tmdb_id=None))
    assert get_meta(client, "movie", "tt0000013")["meta"]["moviedb_id"] == "13"
    assert "moviedb_id" not in get_meta(client, "movie", "tt0000014")["meta"]


async def test_rating_is_rounded_and_unrated_is_omitted(client, seed):
    await seed.meta(meta_doc("tmdb:15", imdb_id="tt0000015",
                             aliases=["tmdb:15", "tt0000015"], rating=8.708))
    await seed.meta(meta_doc("tmdb:16", imdb_id="tt0000016",
                             aliases=["tmdb:16", "tt0000016"], rating=None))
    assert get_meta(client, "movie", "tt0000015")["meta"]["imdbRating"] == "8.7"
    assert get_meta(client, "movie", "tt0000016")["meta"]["imdbRating"] == ""


async def test_null_genres_and_images_do_not_leak(client, seed):
    await seed.meta(meta_doc("tmdb:17", imdb_id="tt0000017",
                             aliases=["tmdb:17", "tt0000017"],
                             genres=None, poster=None, background=None,
                             description=None))
    m = get_meta(client, "movie", "tt0000017")["meta"]
    assert m["genres"] == []
    assert m["poster"] == ""
    assert m["background"] == ""
    assert m["description"] == ""
    assert spec.check_meta(get_meta(client, "movie", "tt0000017"), "movie") == []


async def test_relative_image_paths_are_dropped(client, seed):
    """poster/background are parsed with Url::parse — a relative path would
    make stremio-core discard the entry."""
    await seed.meta(meta_doc("tmdb:18", imdb_id="tt0000018",
                             aliases=["tmdb:18", "tt0000018"],
                             poster="/images/p.jpg", background="notaurl"))
    m = get_meta(client, "movie", "tt0000018")["meta"]
    assert m["poster"] == "" and m["background"] == ""


async def test_empty_title_still_produces_a_usable_name(client, seed):
    """`name` is required; an empty one dropped the catalog entry entirely."""
    await seed.meta(meta_doc("tmdb:4444", imdb_id="tt4444444",
                             aliases=["tmdb:4444", "tt4444444"],
                             title="", tmdb_id=4444))
    await seed.catalogs()
    await seed.files(file_doc("tmdb:4444", -1001, 20, "Untitled.mkv"))
    body = get_meta(client, "movie", "tt4444444")
    assert body["meta"]["name"]
    assert spec.check_meta(body, "movie", "tt4444444") == []
    assert get_catalog(client, "movie", "other_movies")["metas"][0]["name"]


# ---------------------------------------------------------------------------
# Id resolution
# ---------------------------------------------------------------------------


async def test_shared_imdb_id_gets_a_unique_catalog_id(client, seed):
    """Two titles sharing one IMDb id used to produce two identical catalog
    ids; both then resolved to whichever doc Mongo returned first."""
    await seed.meta(meta_doc("tmdb:1111", imdb_id="tt5555555",
                             aliases=["tmdb:1111", "tt5555555"],
                             title="Original 1985 Film", year=1985,
                             catalog="tamil_movies", updated_at=50.0))
    await seed.meta(meta_doc("tmdb:2222", imdb_id="tt5555555",
                             aliases=["tmdb:2222", "tt5555555"],
                             title="2024 Remake", year=2024,
                             catalog="tamil_movies", updated_at=200.0))
    await seed.files(file_doc("tmdb:1111", -1001, 30, "Original.1985.mkv"))
    await seed.files(file_doc("tmdb:2222", -1001, 31, "Remake.2024.mkv"))
    await seed.catalogs()

    metas = get_catalog(client, "movie", "tamil_movies")["metas"]
    ids = {m["id"] for m in metas}
    assert ids == {"tmdb:1111", "tmdb:2222"}, ids
    assert spec.check_catalog({"metas": metas}) == []

    # each id now opens the title it actually belongs to
    by_id = {m["id"]: m["name"] for m in metas}
    for cid, cname in by_id.items():
        assert get_meta(client, "movie", cid)["meta"]["name"] == cname
        assert get_streams(client, "movie", cid)


async def test_unambiguous_imdb_id_is_still_used(client, seed):
    """Keeping the tt… id matters: it lets Cinemeta enrich the page and keeps
    IMDb deep links working, so it must not be replaced wholesale."""
    await seed.meta(meta_doc("tmdb:278", imdb_id="tt0111161",
                             aliases=["tmdb:278", "tt0111161"]))
    await seed.files(file_doc("tmdb:278", -1001, 40, "Shawshank.mkv"))
    await seed.catalogs()
    metas = get_catalog(client, "movie", "other_movies")["metas"]
    assert metas[0]["id"] == "tt0111161"


async def test_movie_and_series_sharing_an_imdb_id_resolve_by_type(client, seed):
    await seed.meta(meta_doc("tmdb:21", imdb_id="tt7777777",
                             aliases=["tmdb:21", "tt7777777"],
                             title="The Movie", media_type="movie",
                             catalog="other_movies", updated_at=10.0))
    await seed.meta(meta_doc("tmdb:22", imdb_id="tt7777777",
                             aliases=["tmdb:22", "tt7777777"],
                             title="The Series", media_type="series",
                             catalog="other_series", updated_at=99.0))
    assert get_meta(client, "movie", "tt7777777")["meta"]["name"] == "The Movie"
    assert get_meta(client, "series", "tt7777777")["meta"]["name"] == "The Series"


async def test_unknown_id_returns_empty_meta_and_is_not_cached(client, seed):
    """A transient miss must not be pinned in the meta cache for META_TTL."""
    from app.cache import meta_cache

    empty = get_meta(client, "movie", "tt0000000")
    assert empty == {"meta": {}}
    assert meta_cache.get((TOKEN, "movie", "tt0000000")) is None

    await seed.meta(meta_doc("tmdb:31", imdb_id="tt0000000",
                             aliases=["tmdb:31", "tt0000000"], title="Arrived Late"))
    assert get_meta(client, "movie", "tt0000000")["meta"]["name"] == "Arrived Late"


# ---------------------------------------------------------------------------
# Series episode list
# ---------------------------------------------------------------------------


async def test_season_pack_only_series_is_still_playable(client, seed):
    """meta.videos == [] left a series page with no episodes at all whenever
    the TMDb expansion could not run (no API key / rate limit / offline)."""
    await seed.meta(meta_doc("tmdb:777", imdb_id="tt0000777",
                             aliases=["tmdb:777", "tt0000777"],
                             title="Bigg Boss Tamil", media_type="series",
                             catalog="tamil_series"))
    await seed.files(file_doc("tmdb:777", -1002, 50,
                              "Bigg.Boss.Tamil.10.Season.10.Complete.720p.mkv",
                              season=10, episode_start=None, episode_end=None))
    body = get_meta(client, "series", "tt0000777")
    assert spec.check_meta(body, "series", "tt0000777") == []
    videos = body["meta"]["videos"]
    assert videos and videos[0]["season"] == 10
    assert get_streams(client, "series", videos[0]["id"])


async def test_episode_season_numbers_are_unsigned_ints(client, seed):
    """stremio-core deserializes season/episode into u32."""
    await seed.meta(meta_doc("tmdb:888", imdb_id="tt0000888",
                             aliases=["tmdb:888", "tt0000888"],
                             media_type="series", catalog="other_series"))
    await seed.files(file_doc("tmdb:888", -1002, 60, "Show.S01E01.mkv",
                              season=1, episode_start=1, episode_end=2))
    for v in get_meta(client, "series", "tt0000888")["meta"]["videos"]:
        assert isinstance(v["season"], int) and v["season"] >= 0
        assert isinstance(v["episode"], int) and v["episode"] >= 0


async def test_legacy_list_shaped_season_and_episodes(client, seed):
    await seed.meta(meta_doc("tmdb:889", imdb_id="tt0000889",
                             aliases=["tmdb:889", "tt0000889"],
                             media_type="series", catalog="other_series"))
    await seed.files(file_doc("tmdb:889", -1002, 61, "Show.S01E03.mkv",
                              season=[1], episode_start=[3], episode_end=[3]))
    body = get_meta(client, "series", "tt0000889")
    assert spec.check_meta(body, "series", "tt0000889") == []
    assert [v["episode"] for v in body["meta"]["videos"]] == [3]


# ---------------------------------------------------------------------------
# Streams
# ---------------------------------------------------------------------------


async def test_filename_with_a_slash_does_not_break_the_dl_url(client, seed):
    """/dl/{token}/{sid}/{name} has exactly three path segments; an unescaped
    "/" in the filename added a fourth and 404'd the stream."""
    await seed.meta(meta_doc("tmdb:603", imdb_id="tt0133093",
                             aliases=["tmdb:603", "tt0133093"]))
    await seed.files(file_doc("tmdb:603", -1001, 70,
                              "The Matrix 1999 720p/backup copy.mkv"))
    streams = get_streams(client, "movie", "tt0133093")
    assert len(streams) == 1
    tail = streams[0]["url"].split("/dl/")[1]
    assert tail.count("/") == 2, streams[0]["url"]
    assert spec.check_streams({"streams": streams}) == []


async def test_video_size_lives_in_behavior_hints(client, seed):
    """The spec puts videoSize under behaviorHints (that is what subtitle
    addons receive); a top-level videoSize is ignored by Stremio."""
    await seed.meta(meta_doc("tmdb:278", imdb_id="tt0111161",
                             aliases=["tmdb:278", "tt0111161"]))
    await seed.files(file_doc("tmdb:278", -1001, 80, "Shawshank.1080p.mkv",
                              size=1234567))
    s = get_streams(client, "movie", "tt0111161")[0]
    assert "videoSize" not in s
    assert s["behaviorHints"]["videoSize"] == 1234567
    assert s["behaviorHints"]["filename"] == "Shawshank.1080p.mkv"
    assert spec.check_streams({"streams": [s]}) == []


async def test_stream_url_is_absolute_and_parseable(client, seed):
    await seed.meta(meta_doc("tmdb:278", imdb_id="tt0111161",
                             aliases=["tmdb:278", "tt0111161"]))
    await seed.files(file_doc("tmdb:278", -1001, 90, "Shawshank.1080p.mkv"))
    s = get_streams(client, "movie", "tt0111161")[0]
    assert s["url"].startswith("https://addon.example/dl/")
    assert spec.check_streams({"streams": [s]}) == []


# ---------------------------------------------------------------------------
# Whole-library sweep
# ---------------------------------------------------------------------------


async def test_every_catalog_entry_opens_and_plays(client, seed):
    """The click path end to end: catalog -> meta -> stream, for every shape
    the DB can hold at once."""
    await seed.catalogs()
    await seed.meta(
        meta_doc("tmdb:278", imdb_id="tt0111161", aliases=["tmdb:278", "tt0111161"],
                 title="The Shawshank Redemption", year=1994, rating=8.708,
                 trailer_yt="NmzuHjWmXOc", catalog="other_movies"),
        meta_doc("tmdb:30977", imdb_id=None, aliases=["tmdb:30977"],
                 title="Kamen Rider W", year=None, genres=None, poster=None,
                 background=None, description=None, rating=None,
                 catalog="anime_movies"),
        meta_doc("tmdb:1399", imdb_id="tt0944947", aliases=["tmdb:1399", "tt0944947"],
                 title="Game of Thrones", year=2011, media_type="tv",
                 catalog="other_series"),
        meta_doc("song:tmdb:87612", imdb_id="song:tt0234215",
                 aliases=["song:tmdb:87612", "song:tt0234215"],
                 title="Nenjunjal Kudi (Video Songs)", catalog="video_songs"),
        meta_doc("tmdb:5555", imdb_id="tt5555556", aliases=["tmdb:5555"],
                 title="Orphan (no files)", catalog="other_movies"),
    )
    await seed.files(
        file_doc("tmdb:278", -1001, 1, "Shawshank.1994.1080p.BluRay.mkv"),
        file_doc("tmdb:30977", -1001, 2, "Kamen.Rider.W.720p.mkv"),
        file_doc("tmdb:1399", -1002, 1, "GoT.S01E01.720p.mkv",
                 season=1, episode_start=1, episode_end=1),
        file_doc("tmdb:1399", -1002, 2, "GoT.S01 Ep 02 - 04.mkv",
                 season=1, episode_start=2, episode_end=4),
        file_doc("song:tmdb:87612", -1001, 3, "Nenjunjal Kudi Songs.mkv"),
        file_doc("tmdb:6666", -1003, 1, "Orphan file, meta deleted.mkv"),
    )

    problems = []
    entries = []
    for cat in ("other_movies", "anime_movies", "other_series", "video_songs",
                "tamil_movies", "tamil_series", "dubbed_movies", "dubbed_series",
                "anime_series", "other_series"):
        mtype = "series" if cat.endswith("series") else "movie"
        body = get_catalog(client, mtype, cat)
        problems += [f"catalog {cat}: {e}" for e in spec.check_catalog(body)]
        for m in body.get("metas") or []:
            entries.append((cat, mtype, m["id"], m.get("name")))

    # the orphan title must never be listed
    assert all(name != "Orphan (no files)" for _, _, _, name in entries)

    for cat, mtype, cid, cname in entries:
        body = get_meta(client, mtype, cid)
        problems += [f"meta {mtype}/{cid} ({cname}): {e}"
                     for e in spec.check_meta(body, mtype, cid)]
        r = client.get(f"/stremio/{TOKEN}/stream/{mtype}/{cid}.json")
        problems += [f"stream {mtype}/{cid} ({cname}): {e}"
                     for e in spec.check_streams(r.json())]
        if not r.json()["streams"]:
            problems.append(f"stream {mtype}/{cid} ({cname}): listed in {cat} "
                            f"but has no streams")
        for v in (body.get("meta") or {}).get("videos") or []:
            er = client.get(f"/stremio/{TOKEN}/stream/{mtype}/{v['id']}.json")
            problems += [f"episode {v['id']}: {e}"
                         for e in spec.check_streams(er.json())]
            if not er.json()["streams"]:
                problems.append(f"episode {v['id']} ({cname}): advertised in "
                                f"meta.videos but has no streams")

    assert problems == []
