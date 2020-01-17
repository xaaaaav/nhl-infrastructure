import json

from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession as ss
from pyspark.sql.functions import col, explode, split
from pyspark.sql.types import DoubleType, StringType

EVENTS = [
  'FACEOFF',
  'HIT',
  'BLOCKED_SHOT',
  'PENALTY',
  'SHOT',
  'GIVEAWAY',
  'GOAL',
  'MISSED_SHOT',
  'TAKEAWAY'
]

EVENT_FUNC_MAPPING = {
    'FACEOFF': "flatten_faceoff_data",
    'HIT': "flatten_hit_data",
    'BLOCKED_SHOT': "flatten_blocked_shot_data",
    'PENALTY': "flatten_penalty_data",
    'SHOT': "flatten_shot_data",
    'GIVEAWAY': "flatten_giveaway_data",
    'GOAL': "flatten_goal_data",
    'MISSED_SHOT': "flatten_missed_shot_data",
    'TAKEAWAY': "flatten_takeaway_data"
}

INPUT_PATH = "s3://nhl-player-gamelog/game_data/*/*.json.gz"

def build_rdd_by_event(sc):
    rdds = {}

    for event in EVENTS:
        rdd = sc.textFile(INPUT_PATH) \
            .map(json.loads) \
            .flatMap(grab_game_info) \
            .filter(lambda x: x["result"]["eventTypeId"] == event) \
            .map(json.dumps) \
            .coalesce(50)
        rdds[event] = rdd
    return rdds

def flatten_faceoff_data(data):
    faceoff_flat = data \
        .select("about", "datetime", "game", explode("players").alias("players"), "result", "team", "coordinates") \
        .select(
            col("game.pk").alias("gameId"),
            "game.season",
            col("game.type").alias("gameType"),
            col("datetime.dateTime").alias("gameStartTime"),
            col("datetime.endDateTime").alias("gameEndTime"),
            col("about.dateTime").alias("eventTime"),
            "about.eventId",
            "about.eventIdx",
            col("about.goals.away").alias("awayGoals"),
            col("about.goals.home").alias("homeGoals"),
            "about.ordinalNum",
            "about.period",
            "about.periodTime",
            "about.periodTimeRemaining",
            "about.periodType",
            col("players.player.fullName").alias("playerName"),
            col("players.player.id").alias("playerId"),
            col("players.player.link").alias("playerLink"),
            "players.playerType",
            "result.description",
            "result.event",
            "result.eventCode",
            "result.eventTypeId",
            col("team.id").alias("teamId"),
            col("team.link").alias("teamLink"),
            col("team.name").alias("teamName"),
            col("team.triCode").alias("teamTriCode"),
            col("coordinates.x").alias("coordinateX"),
            col("coordinates.y").alias("coordinateY"),
        )

    return faceoff_flat

def flatten_hit_data(data):
    hit_flat = data \
        .select("about", "datetime", "game", explode("players").alias("players"), "result", "team", "coordinates") \
        .select(
            col("game.pk").alias("gameId"),
            "game.season",
            col("game.type").alias("gameType"),
            col("datetime.dateTime").alias("gameStartTime"),
            col("datetime.endDateTime").alias("gameEndTime"),
            col("about.dateTime").alias("eventTime"),
            "about.eventId",
            "about.eventIdx",
            col("about.goals.away").alias("awayGoals"),
            col("about.goals.home").alias("homeGoals"),
            "about.ordinalNum",
            "about.period",
            "about.periodTime",
            "about.periodTimeRemaining",
            "about.periodType",
            col("players.player.fullName").alias("playerName"),
            col("players.player.id").alias("playerId"),
            col("players.player.link").alias("playerLink"),
            "players.playerType",
            "result.description",
            "result.event",
            "result.eventCode",
            "result.eventTypeId",
            col("team.id").alias("teamId"),
            col("team.link").alias("teamLink"),
            col("team.name").alias("teamName"),
            col("team.triCode").alias("teamTriCode"),
            col("coordinates.x").alias("coordinateX"),
            col("coordinates.y").alias("coordinateY"),
        )

    return hit_flat

def flatten_blocked_shot_data(data):
    blocked_shot_flat = data \
        .select("about", "datetime", "game", explode("players").alias("players"), "result", "team", "coordinates") \
        .select(
            col("game.pk").alias("gameId"),
            "game.season",
            col("game.type").alias("gameType"),
            col("datetime.dateTime").alias("gameStartTime"),
            col("datetime.endDateTime").alias("gameEndTime"),
            col("about.dateTime").alias("eventTime"),
            "about.eventId",
            "about.eventIdx",
            col("about.goals.away").alias("awayGoals"),
            col("about.goals.home").alias("homeGoals"),
            "about.ordinalNum",
            "about.period",
            "about.periodTime",
            "about.periodTimeRemaining",
            "about.periodType",
            col("players.player.fullName").alias("playerName"),
            col("players.player.id").alias("playerId"),
            col("players.player.link").alias("playerLink"),
            "players.playerType",
            "result.description",
            "result.event",
            "result.eventCode",
            "result.eventTypeId",
            col("team.id").alias("teamId"),
            col("team.link").alias("teamLink"),
            col("team.name").alias("teamName"),
            col("team.triCode").alias("teamTriCode"),
            col("coordinates.x").alias("coordinateX"),
            col("coordinates.y").alias("coordinateY"),
        )

    return blocked_shot_flat

def flatten_penalty_data(data):
    penalty_flat = data \
        .select("about", "datetime", "game", explode("players").alias("players"), "result", "team", "coordinates") \
        .select(
            col("game.pk").alias("gameId"),
            "game.season",
            col("game.type").alias("gameType"),
            col("datetime.dateTime").alias("gameStartTime"),
            col("datetime.endDateTime").alias("gameEndTime"),
            col("about.dateTime").alias("eventTime"),
            "about.eventId",
            "about.eventIdx",
            col("about.goals.away").alias("awayGoals"),
            col("about.goals.home").alias("homeGoals"),
            "about.ordinalNum",
            "about.period",
            "about.periodTime",
            "about.periodTimeRemaining",
            "about.periodType",
            col("players.player.fullName").alias("playerName"),
            col("players.player.id").alias("playerId"),
            col("players.player.link").alias("playerLink"),
            "players.playerType",
            "result.description",
            "result.event",
            "result.eventCode",
            "result.eventTypeId",
            "result.penaltyMinutes",
            "result.penaltySeverity",
            "result.secondaryType",
            col("team.id").alias("teamId"),
            col("team.link").alias("teamLink"),
            col("team.name").alias("teamName"),
            col("team.triCode").alias("teamTriCode"),
            col("coordinates.x").alias("coordinateX"),
            col("coordinates.y").alias("coordinateY"),
        )

    return penalty_flat

def flatten_shot_data(data):
    shot_flat = data \
        .select("about", "datetime", "game", explode("players").alias("players"), "result", "team", "coordinates") \
        .select(
            col("game.pk").alias("gameId"),
            "game.season",
            col("game.type").alias("gameType"),
            col("datetime.dateTime").alias("gameStartTime"),
            col("datetime.endDateTime").alias("gameEndTime"),
            col("about.dateTime").alias("eventTime"),
            "about.eventId",
            "about.eventIdx",
            col("about.goals.away").alias("awayGoals"),
            col("about.goals.home").alias("homeGoals"),
            "about.ordinalNum",
            "about.period",
            "about.periodTime",
            "about.periodTimeRemaining",
            "about.periodType",
            col("players.player.fullName").alias("playerName"),
            col("players.player.id").alias("playerId"),
            col("players.player.link").alias("playerLink"),
            "players.playerType",
            "result.description",
            "result.event",
            "result.eventCode",
            "result.eventTypeId",
            "result.secondaryType",
            col("team.id").alias("teamId"),
            col("team.link").alias("teamLink"),
            col("team.name").alias("teamName"),
            col("team.triCode").alias("teamTriCode"),
            col("coordinates.x").alias("coordinateX"),
            col("coordinates.y").alias("coordinateY"),
        )

    return shot_flat

def flatten_giveaway_data(data):
    giveaway_flat = data \
        .select("about", "datetime", "game", explode("players").alias("players"), "result", "team", "coordinates") \
        .select(
            col("game.pk").alias("gameId"),
            "game.season",
            col("game.type").alias("gameType"),
            col("datetime.dateTime").alias("gameStartTime"),
            col("datetime.endDateTime").alias("gameEndTime"),
            col("about.dateTime").alias("eventTime"),
            "about.eventId",
            "about.eventIdx",
            col("about.goals.away").alias("awayGoals"),
            col("about.goals.home").alias("homeGoals"),
            "about.ordinalNum",
            "about.period",
            "about.periodTime",
            "about.periodTimeRemaining",
            "about.periodType",
            col("players.player.fullName").alias("playerName"),
            col("players.player.id").alias("playerId"),
            col("players.player.link").alias("playerLink"),
            "players.playerType",
            "result.description",
            "result.event",
            "result.eventCode",
            "result.eventTypeId",
            col("team.id").alias("teamId"),
            col("team.link").alias("teamLink"),
            col("team.name").alias("teamName"),
            col("team.triCode").alias("teamTriCode"),
            col("coordinates.x").alias("coordinateX"),
            col("coordinates.y").alias("coordinateY"),
        )

    return giveaway_flat

def flatten_goal_data(data):
    goal_flat = data \
        .select("about", "datetime", "game", explode("players").alias("players"), "result", "team", "coordinates") \
        .select(
            col("game.pk").alias("gameId"),
            "game.season",
            col("game.type").alias("gameType"),
            col("datetime.dateTime").alias("gameStartTime"),
            col("datetime.endDateTime").alias("gameEndTime"),
            col("about.dateTime").alias("eventTime"),
            "about.eventId",
            "about.eventIdx",
            col("about.goals.away").alias("awayGoals"),
            col("about.goals.home").alias("homeGoals"),
            "about.ordinalNum",
            "about.period",
            "about.periodTime",
            "about.periodTimeRemaining",
            "about.periodType",
            col("players.player.fullName").alias("playerName"),
            col("players.player.id").alias("playerId"),
            col("players.player.link").alias("playerLink"),
            "players.playerType",
            "players.seasonTotal",
            "result.description",
            "result.emptyNet",
            "result.event",
            "result.eventCode",
            "result.eventTypeId",
            "result.gameWinningGoal",
            "result.secondaryType",
            col("result.strength.code").alias("strengthCode"),
            col("result.strength.name").alias("strengthName"),
            col("team.id").alias("teamId"),
            col("team.link").alias("teamLink"),
            col("team.name").alias("teamName"),
            col("team.triCode").alias("teamTriCode"),
            col("coordinates.x").alias("coordinateX"),
            col("coordinates.y").alias("coordinateY"),
        )

    return goal_flat

def flatten_missed_shot_data(data):
    missed_shot_flat = data \
        .select("about", "datetime", "game", explode("players").alias("players"), "result", "team", "coordinates") \
        .select(
            col("game.pk").alias("gameId"),
            "game.season",
            col("game.type").alias("gameType"),
            col("datetime.dateTime").alias("gameStartTime"),
            col("datetime.endDateTime").alias("gameEndTime"),
            col("about.dateTime").alias("eventTime"),
            "about.eventId",
            "about.eventIdx",
            col("about.goals.away").alias("awayGoals"),
            col("about.goals.home").alias("homeGoals"),
            "about.ordinalNum",
            "about.period",
            "about.periodTime",
            "about.periodTimeRemaining",
            "about.periodType",
            col("players.player.fullName").alias("playerName"),
            col("players.player.id").alias("playerId"),
            col("players.player.link").alias("playerLink"),
            "players.playerType",
            "result.description",
            "result.event",
            "result.eventCode",
            "result.eventTypeId",
            col("team.id").alias("teamId"),
            col("team.link").alias("teamLink"),
            col("team.name").alias("teamName"),
            col("team.triCode").alias("teamTriCode"),
            col("coordinates.x").alias("coordinateX"),
            col("coordinates.y").alias("coordinateY"),
        )

    return missed_shot_flat

def flatten_takeaway_data(data):
    takeaway_flat = data \
        .select("about", "datetime", "game", explode("players").alias("players"), "result", "team", "coordinates") \
        .select(
            col("game.pk").alias("gameId"),
            "game.season",
            col("game.type").alias("gameType"),
            col("datetime.dateTime").alias("gameStartTime"),
            col("datetime.endDateTime").alias("gameEndTime"),
            col("about.dateTime").alias("eventTime"),
            "about.eventId",
            "about.eventIdx",
            col("about.goals.away").alias("awayGoals"),
            col("about.goals.home").alias("homeGoals"),
            "about.ordinalNum",
            "about.period",
            "about.periodTime",
            "about.periodTimeRemaining",
            "about.periodType",
            col("players.player.fullName").alias("playerName"),
            col("players.player.id").alias("playerId"),
            col("players.player.link").alias("playerLink"),
            "players.playerType",
            "result.description",
            "result.event",
            "result.eventCode",
            "result.eventTypeId",
            col("team.id").alias("teamId"),
            col("team.link").alias("teamLink"),
            col("team.name").alias("teamName"),
            col("team.triCode").alias("teamTriCode"),
            col("coordinates.x").alias("coordinateX"),
            col("coordinates.y").alias("coordinateY"),
        )

    return takeaway_flat

def grab_game_info(x):
    data = x["liveData"]["plays"]["allPlays"]
    for d in data:
        d["datetime"] = x["gameData"]["datetime"]
        d["game"] = x["gameData"]["game"]
    return data

def run():
    conf = SparkConf().setAppName("Hockey Analysis")
    sc = SparkContext(conf=conf)
    rdds = build_rdd_by_event(sc)
    sqlContext = ss.builder \
         .master("local") \
         .appName("Hockey Analysis") \
         .getOrCreate()

    for event in EVENTS:
        df = sqlContext.read.json(rdds[event])
        flattened_df = eval(EVENT_FUNC_MAPPING[event] + "("df")")

        path = "s3a://nhl-player-gamelog/game_data/flattened/{}.parquet".format(event)
        flattened_df.write.parquet(path, mode="overwrite")

run()