import argparse
import apache_beam as beam
from datetime import datetime
from apache_beam.options.pipeline_options import PipelineOptions


def calculate_points(element):
    customer_id, first_name, last_name, realtionship_id, card_type, max_limit, spent, cash_withdrawn, payment_cleared, payment_date = element.split(
        ',')
    # [CT28383,Miyako,Burns,R_7488,Issuers,500,490,38,101,30-01-2018]

    spent = int(spent)  # spent = 490
    payment_cleared = int(payment_cleared)  # payment_cleared = 101
    max_limit = int(max_limit)  # max_limit = 500

    key_name = customer_id + ', ' + first_name + ' ' + last_name  # key_name = CT28383,Miyako Burns
    defaulter_points = 0

    # payment_cleared is less than 70% of spent - give 1 point
    if payment_cleared < (spent * 0.7):
        defaulter_points += 1  # defaulter_points =  1

    # spend is = 100% of max limit and any amount of payment is pending
    if (spent == max_limit) and (payment_cleared < spent):
        defaulter_points += 1  # defaulter_points =  2

    if (spent == max_limit) and (payment_cleared < (spent * 0.7)):
        defaulter_points += 1  # defaulter_points = 3

    return key_name, defaulter_points


def is_medical_defaulter(record):
    due_date = datetime.strptime(record[6].strip(), "%d-%m-%Y")
    payment_date = datetime.strptime(record[-1].strip(), "%d-%m-%Y")
    if payment_date > due_date:
        record.append(1)
    else:
        record.append(0)
    return record


def format_defaulters(record):
    return record[0] + " has " + str(record[1]) + " defaults"


def format_result(sum_pair):
    key_name, points = sum_pair
    return str(key_name) + ', ' + str(points) + ' fraud_points'


def extract_month(record):
    payment_date = datetime.strptime(record[-1].strip(), "%d-%m-%Y")
    return record[0] + ", " + record[1] + " " + record[2], payment_date.month


def calc_num_defaults(record):
    key, months = record
    months = list(months) # https://stackoverflow.com/questions/41892701/beam-dataflow-python-attributeerror-unwindowedvalues-object-has-no-attribut
    months.sort(reverse=False)
    max_total_defaults = 4
    max_consecutive_defaults = 2
    total_defaults = 12 - len(months)

    if total_defaults >= max_total_defaults:
        return key, total_defaults

    consecutive_defaults = 0
    tmp = months[0] - 1
    if tmp > consecutive_defaults:
        consecutive_defaults = tmp

    tmp = 12 - months[-1]
    if tmp > consecutive_defaults:
        consecutive_defaults = tmp

    for i in range(1, len(months)):
        tmp = months[i] - months[i - 1] - 1
        if tmp > consecutive_defaults:
            consecutive_defaults = tmp

    return key, consecutive_defaults


def sorting_output(record):
    key, sort_data = record
    sort_data = list(sort_data)
    sort_data.sort(key=lambda x: x[1], reverse=True)
    return sort_data[:10]


class MyTransform(beam.PTransform):
    def expand(self, input_collection):
        return (
                input_collection
                | "Mapping each record with 1" >> beam.Map(lambda record: (1, record))
                | "Grouping all records to a single record" >> beam.GroupByKey()
                | "Sorting Records and getting to new line" >> beam.FlatMap(sorting_output)
                | "Formatting output" >> beam.Map(format_defaulters)

        )


parser = argparse.ArgumentParser()

parser.add_argument('--input1',
                    dest='input1',
                    required=True,
                    help='Input card data file to process.')
parser.add_argument('--input2',
                    dest='input2',
                    required=True,
                    help='Input loan data file to process.')
parser.add_argument('--output1',
                    dest='output1',
                    required=True,
                    help='Output card defaulter file to write results to.')
parser.add_argument('--output2',
                    dest='output2',
                    required=True,
                    help='Output loan defaulter file to write results to.')

path_args, pipeline_args = parser.parse_known_args()

inputs_pattern1 = path_args.input1
inputs_pattern2 = path_args.input2
outputs_prefix1 = path_args.output1
outputs_prefix2 = path_args.output2

options = PipelineOptions(pipeline_args)
p = beam.Pipeline(options=options)

card_defaulter = (
      p | 'Read credit card data' >> beam.io.ReadFromText(inputs_pattern1, skip_header_lines=1)
        | 'Calculate defaulter points' >> beam.Map(calculate_points)
        | 'Combine points for defaulters' >> beam.CombinePerKey(sum)
        | 'Filter card defaulters' >> beam.Filter(lambda element: element[1] > 0)
        | 'Format output' >> beam.Map(format_result)
        | 'Write credit card data' >> beam.io.WriteToText(outputs_prefix1)
)


medical_loan_defaulters = (
      p | "First Data Read" >> beam.io.ReadFromText(inputs_pattern2, skip_header_lines=1)
        | beam.Map(lambda record: record.split(","))
        | beam.Filter(lambda record: record[5].strip() == "Medical Loan")
        | beam.Map(is_medical_defaulter)
        | beam.Map(lambda record: (record[0] + ", " + record[1] + " " + record[2], record[-1]))
        | beam.CombinePerKey(sum)
        | "First use of custom transform" >> MyTransform()
)

personal_loan_defaulters = (
      p | "Second Data Read" >> beam.io.ReadFromText(inputs_pattern2, skip_header_lines=1)
        | beam.Map(lambda record: record.split(","))
        | beam.Filter(lambda record: record[5].strip() == "Personal Loan")
        | beam.Map(extract_month)
        | beam.GroupByKey()
        | beam.Map(calc_num_defaults)
        | "Second use of custom transform" >> MyTransform()
)

all_defaulters = (
      (medical_loan_defaulters, personal_loan_defaulters) | beam.Flatten()
                                                          | beam.io.WriteToText(outputs_prefix2)
)

p.run()
