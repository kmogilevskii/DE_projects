import os
import apache_beam as beam
from apache_beam.transforms import window
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions
from apache_beam.transforms.trigger import Repeatedly, AfterCount, AccumulationMode

path_service_account = '#############'

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = path_service_account

input_subscription = "################"

output_topic_player = '##############'
output_topic_team   = '##############'
output_topic_weapon = '##############'

options = PipelineOptions()
options.view_as(StandardOptions).streaming = True

p = beam.Pipeline(options=options)


def encode_byte_string(element):
    element = str(element)
    return element.encode('utf-8')


def calculate_kill_points(record):
    total_points = 0
    game_id = record[0].strip()
    killer_id = record[1].strip()
    kill_weapon_name = record[4].strip()

    fight_time = int(record[15].strip())

    if 10 <= fight_time <= 20:
        total_points += 4
    elif 21 <= fight_time <= 30:
        total_points += 3
    elif 31 <= fight_time <= 40:
        total_points += 2
    else:
        total_points += 1

    kill_weapon_rank = int(record[6].strip())
    defeated_weapon_rank = int(record[13].strip())
    rank_diff = kill_weapon_rank - defeated_weapon_rank

    if rank_diff > 6:
        total_points += 3
    elif rank_diff > 3:
        total_points += 2
    else:
        total_points += 1

    kill_place = record[7].strip()
    defeat_place = record[14].strip()

    if kill_place != defeat_place:
        total_points += 3

    return game_id + ":" + killer_id + ":" + kill_weapon_name, total_points


def format_output(record):
    return record[0] + " " + str(record[1]) + " average points."


class AverageFn(beam.CombineFn):

    def create_accumulator(self):
        return 0.0, 0

    def add_input(self, sum_count, input):
        return sum_count[0] + input, sum_count[1] + 1

    def merge_accumulators(self, accumulators):
        ind_sum, ind_count = zip(*accumulators)
        return sum(ind_sum), sum(ind_count)

    def extract_output(self, accumulator):
        return accumulator[0] / accumulator[1] if accumulator[1] else float("NaN")


prepared_data = (
        p | "Read From Pub Sub" >> beam.io.ReadFromPubSub(subscription=input_subscription)
        | "Remove Extra Characters" >> beam.Map(lambda record: record.strip())
        | "Decode and Split" >> beam.Map(lambda record: record.decode().split(","))
)

player_scores = (
        prepared_data | "Player - Kill mapping" >> beam.Map(lambda record: (record[1], 1))
        | "Windowing Player - Kill mapping" >> beam.WindowInto(window.GlobalWindows(),
                                                               trigger=Repeatedly(AfterCount(10)),
                                                               accumulation_mode=AccumulationMode.ACCUMULATING)
        | "Combine Player's Kills" >> beam.CombinePerKey(sum)
        | "Preparing for send Players" >> beam.Map(encode_byte_string)
        | "Player's mapping to pubsub topic" >> beam.io.WriteToPubSub(output_topic_player)
)

team_scores = (
        prepared_data | "Team - Kill mapping" >> beam.Map(lambda record: (record[0] + ", " + record[3], 1))
        | "Windowing Team - Kill mapping" >> beam.WindowInto(window.GlobalWindows(),
                                                             trigger=Repeatedly(AfterCount(10)),
                                                             accumulation_mode=AccumulationMode.ACCUMULATING)
        | "Combine Team's Kills" >> beam.CombinePerKey(sum)
        | "Preparing for send Teams" >> beam.Map(encode_byte_string)
        | "Team's mapping to pubsub topic" >> beam.io.WriteToPubSub(output_topic_team)
)

weapon_scores = (
        prepared_data | "Calculating kill points" >> beam.Map(calculate_kill_points)
        | "Windowing Weapon stats" >> beam.WindowInto(window.Sessions(30))
        | "Computing average point" >> beam.CombinePerKey(AverageFn())
        | "Format output" >> beam.Map(format_output)
        | "Preparing for Send Weapons" >> beam.Map(encode_byte_string)
        | "Weapon's stats to pubsub topic" >> beam.io.WriteToPubSub(output_topic_weapon)
)

result = p.run()
result.wait_until_finish()
