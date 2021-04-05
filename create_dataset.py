import random
from arrow.arrow import Arrow
from sortedcontainers import SortedList
import arrow
from config import *
import cassiopeia as cass
from cassiopeia.core import Summoner, MatchHistory, Match
from cassiopeia import Queue, Patch
import os


cass.set_riot_api_key(API_KEY)
cass.set_default_region(REGION)

match_fields = {
    "dynamic": [
        "id",
        "duration",
        "is_remake",
    ],
    "static": {
        "patch": PATCH,
        "region": REGION
    },
    "team": ["baron_kills", "dragon_kills", "first_baron", "first_blood", "first_inhibitor", "first_tower"],
    "bans": ["ban_B1", "ban_B2", "ban_B3", "ban_B4", "ban_B5", "ban_R1", "ban_R2", "ban_R3", "ban_R4", "ban_R5"],
    "champions": ["pick_B1", "pick_B2", "pick_B3", "pick_B4", "pick_B5", "pick_R1", "pick_R2", "pick_R3", "pick_R4", "pick_R5"],
}

def setup_files():
    if not os.path.exists(MATCHES_FILE):
        with open(MATCHES_FILE, 'w') as matches_file:
            for field in match_fields["dynamic"] + list(match_fields["static"].keys()) + match_fields["team"] + match_fields["bans"] + match_fields["champions"]:
                print(field, end='\t', file=matches_file)


def handle_print(match: Match):
    # print([match.id, match.duration.total_seconds(), match.is_remake, match.patch.majorminor, match.region.value])
    # print([match.blue_team.baron_kills, match.blue_team.dragon_kills, match.blue_team.first_baron, match.blue_team.first_blood, match.blue_team.first_inhibitor, match.blue_team.first_tower])
    # print([banned_champ.id if type(banned_champ) == Champion else -1 for banned_champ in match.blue_team.bans + match.red_team.bans ])
    # print([participant.champion.id for participant in match.blue_team.participants + match.red_team.participants])
    print([getattr(match, field) for field in match_fields["dynamic"]])

# heavily inspired by: https://github.com/meraki-analytics/cassiopeia/issues/359#issuecomment-787516878
def filter_match_history(summoner: Summoner, patch: Patch) -> MatchHistory:
    end_time: Arrow = patch.end
    if end_time is None:
        end_time = arrow.now()
    match_history: MatchHistory = MatchHistory(summoner=summoner, queues={Queue.ranked_solo_fives}, begin_time=patch.start, end_time=end_time)
    return match_history


def collect_matches():

    summoner: Summoner = Summoner(name=INIT_SUMMONER, region=REGION)
    patch: Patch = Patch.from_str(PATCH, region=REGION)

    unpulled_summoner_ids: SortedList[str] = SortedList([summoner.id])
    pulled_summoner_ids: SortedList[str] = SortedList()

    unpulled_match_ids: SortedList[str] = SortedList()
    pulled_match_ids: SortedList[str] = SortedList()
    i = 0
    j = 0
    while unpulled_summoner_ids and j < 3:
        # Get a random summoner from our list of unpulled summoners and pull their match history
        new_summoner_id: str = random.choice(unpulled_summoner_ids)
        new_summoner: Summoner = Summoner(id=new_summoner_id, region=REGION)
        matches: MatchHistory = filter_match_history(new_summoner, patch)
        unpulled_match_ids.update([match.id for match in matches])
        unpulled_summoner_ids.remove(new_summoner_id)
        pulled_summoner_ids.add(new_summoner_id)
        j = j+1

        # print([match.region.value for match in matches ])
        [handle_print(match) for match in matches]
        # while unpulled_match_ids and i < 3:
        #     # Get a random match from our list of matches
        #     new_match_id: str = random.choice(unpulled_match_ids)
        #     new_match: Match = Match(id=new_match_id, region=REGION)
        #     for participant in new_match.participants:
        #         participant: Participant
        #         if participant.summoner.id not in pulled_summoner_ids and participant.summoner.id not in unpulled_summoner_ids:
        #             unpulled_summoner_ids.add(participant.summoner.id)
        #     # The above lines will trigger the match to load its data by iterating over all the participants.
        #     # If you have a database in your datapipeline, the match will automatically be stored in it.
        #     unpulled_match_ids.remove(new_match_id)
        #     pulled_match_ids.add(new_match_id)
        #     i = i +1




if __name__ == "__main__":
    setup_files()
    collect_matches()