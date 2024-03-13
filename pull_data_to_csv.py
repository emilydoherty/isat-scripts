# This script pulls data from the recorder, MMIA, JIA Agent, and MakeCode and writes it to CSV.

from urllib.parse import urlparse

import boto3
import csv
import os
import pprint
import re

import dateutil
from dateutil import parser
from boto3.dynamodb.conditions import Key
from datetime import datetime, timedelta

# Set env to 'prod' to use the production DBs, 'dev' to use the development/staging DBs
env = 'prod'

# The main function:
def main():

    # Indicate which data to write to the spreadsheet by adding/removing from the list below:
    data_sources = ['mmia', 'jia_agent', 'makecode']

    # List of recording IDs to process
    # recording_ids = [
    #     '6536f1073ad99b001d621789',
    #     '6574bfdeaf3da30029d45689',
    #     # '657df804af3da30029d45851',
    #     # '658376e0af3da30029d45b1d',
    #     # '65b556d91f687a00080c4509',
    #     # '65be993add672200081feb25',
    #     # '65c7cbfb4e4ea100089cee7f',
    #     # '65caad157de66c00088c6056',
    #     # '65d5367d6b93ad0008b00d45',
    #     #
    #     # '65dccaa8f9f94800081fa216',  # this is a short pilot session with MakeCode activity and updated JIA output
    #     #
    #     # # These have the updated JIA output:
    #     # '65dd1af33d676a000969a676',
    #     # '65e1169a7f935400089d241c',
    #     # '65e26aa2e898da000895f84d',
    #     # '65e37d88ca5a1700083fa08c',
    # ]
    recording_ids = [
    '6536f1073ad99b001d621789',
    '6574bfdeaf3da30029d45689',
    '657df804af3da30029d45851',
    '658376e0af3da30029d45b1d',
    # No recording for session 105
    '65b556d91f687a00080c4509',
    '65be993add672200081feb25',
    '65c7cbfb4e4ea100089cee7f',
    '65caad157de66c00088c6056',
    '65d5367d6b93ad0008b00d45',
    '65da41333eb8db000878941e',
    '65dd1af33d676a000969a676',
    # No recording for session 113
    '65e1169a7f935400089d241c',
    '65e26aa2e898da000895f84d',
    '65e37d88ca5a1700083fa08c',
    '65ea5370a718610008c52ed2'
]

    if len(recording_ids) == 0:
        print("No recording ids were specified. Nothing to do.")

    # Loop through each recording ID
    for recording_id in recording_ids:
        data_source_rows = []

        # Add recording MMIA data:
        if 'mmia' in data_sources:
            recording_metrics_rows = pull_recording_metrics(recording_id)
            data_source_rows.append(recording_metrics_rows)

        # Add JIA Agent data:
        if 'jia_agent' in data_sources:
            jia_agent_rows = pull_jia_agent_events(recording_id)
            data_source_rows.append(jia_agent_rows)

        # Add MakeCode data:
        if 'makecode' in data_sources:
            makcode_event_rows = pull_makecode_events(recording_id)
            data_source_rows.append(makcode_event_rows)

        write_cvs_file(data_source_rows, recording_id, data_sources)


def pull_makecode_events(recording_id):

    table_name = 'RecordingMetadata'
    if env == 'dev':
        table_name = 'dev-RecordingMetadata'

    recording_items = query_dynamo(recording_id, table_name)

    rows = []
    if len(recording_items) == 0:
        return rows

    # Determing the session that this recording was part of:
    session_id = recording_items[0]['sessionId']

    # Get all MakeCode items for this session:
    items = query_makecode_dynamo(session_id)

    # Todo: Need to filter/query only those items that have the same studyIds as those registered in the recording/recording_id

    # print(f"num items: {len(items)} sessionId:{session_id} first item: {items[0]}")

    # Include these additional columns:
    columns_to_add = ['event', 'javaScript', 'projectName', 'studyIds', 'eventId']

    for item in items:
        row = {}

        row["timestamp"] = item["timestamp"]
        row["source"] = "makecode"

        # pprint.pprint(item, indent=4)
        for column in columns_to_add:
            if column in item:
                # print(f"adding item {column}:{item[column]}")
                row[f"mc_{column}"] = item[column]
        rows.append(row)

    # pprint.pprint(rows, indent=4)

    return rows


def pull_jia_agent_events(recording_id):

    table_name = 'JiaAgentEventMetrics'
    if env == 'dev':
        table_name = 'JiaAgentEventMetrics' # For now, all data is being written to the prod DB only

    items = query_dynamo(recording_id, table_name)

    # print(f"num items: {len(items)} first item: {items[0]}")

    # Filter to just one item for testing
    # items = items[1:2]

    # pprint.pprint(items, indent=4)

    rows = []

    # Include these additional columns:
    columns_to_add = ['event_start_date', 'agent_response', 'dialogue_state']

    for item in items:
        row = {}

        row["timestamp"] = item["event_start_date"]
        row["source"] = "jia_agent"

        # pprint.pprint(item, indent=4)
        for column in columns_to_add:
            if column in item:
                # print(f"adding item {column}:{item[column]}")
                row[f"jia_{column}"] = item[column]
        rows.append(row)

    # pprint.pprint(rows, indent=4)

    return rows


def pull_recording_metrics(recording_id):
    table_name = 'RecordingChunkMetrics'
    if env == 'dev':
        table_name = 'dev-RecordingChunkMetrics'

    items = query_dynamo(recording_id, table_name)

    # print(f"num items: {len(items)} first item: {items[0]}")

    # Filter to just one item for testing
    # items = items[1:2]

    # pprint.pprint(items, indent=4)

    rows = []

    # Loop through each utterance in each chunk item. Data that are at the chunk-level
    # will be repeated for each utterance
    for item in items:
        # If there is no utterance, create an empty one for this row:
        if len(item['utterances']) == 0:
            item['utterances'] = make_empty_utterances()
        # pprint.pprint(item['utterances'], indent=4)
        for utterance in item['utterances']:
            rows.append(make_recording_chunk_metrics_row(utterance, item))

    return rows


def make_recording_chunk_metrics_row(utterance, item):
    row = {}
    utterance_id = utterance['utterance_id']
    recording_start_date = item['recording_start_date']

    chunk_start_date = add_seconds_to_utc_date(recording_start_date, item['start_sec'])
    chunk_end_date = add_seconds_to_utc_date(recording_start_date, item['end_sec'])

    # if there is no utterance, date information is empty (None):
    utterance_start_date = ''
    utterance_end_date = ''
    if utterance['start_time'] is not None:
        utterance_start_date = add_seconds_to_utc_date(recording_start_date, utterance['start_time'])
    if utterance['end_time'] is not None:
        utterance_end_date = add_seconds_to_utc_date(recording_start_date, utterance['end_time'])

    # Global columns:
    row["timestamp"] = chunk_end_date  # Is this the best timestamp?
    row["source"] = "recording"

    # Todo: Recordings made prior to Tue, 27 Feb 2024 have timestamps that are -7h off. Need to update timestamps here
    row['recording_start_date'] = recording_start_date
    row['chunk_start_date'] = chunk_start_date
    row['utterance_start_date'] = utterance_start_date
    row['utterance_end_date'] = utterance_end_date

    chunk_media_url = item['chunk_media_url']
    try:
        chunk_num = str(re.findall('.*Chunk(.*)\\.webm', chunk_media_url)[0])
    except IndexError:
        chunk_num = ''

    row['chunk_num'] = chunk_num

    # include all columns for the utterance
    if 'amr' not in utterance:  # Some utterances don't have AMR, so enter empty value:
        row['amr'] = ""
    for key, value in utterance.items():
        row[key] = value

    # find the cobi codes for this utterance:
    my_cobi_code = {}
    cobi_codes = item['cobi_key']
    # print(f"cobi_codes: {cobi_codes}")
    for cobi_code in cobi_codes:
        # print(f"cobi_code: {cobi_code}")
        if cobi_code['utterance_id'] == utterance_id:
            my_cobi_code = cobi_code
            break

    for key, value in my_cobi_code.items():
        if key != 'utterance_id':
            row[f'CoBi_{key}'] = value
            # print(f"utterance {key}:{value}")

    # process cps codes
    CPS_codes = item.get('cps_code', [])
    row['cps_maintain'] = (CPS_codes['MAINTAIN'])
    row['cps_neg'] = (CPS_codes['NEG'])
    row['cps_comm'] = (CPS_codes['COMM'])

    # Process content_words
    content_words = item.get('content_words', {})
    non_zero_words = [word for word, value in content_words.items() if value != 0]
    if non_zero_words:
        row['content_word'] = non_zero_words[0]
    else:
        row['content_word'] = ''

    row['chunk_link'] = create_console_link_for_s3_object(item['s3_bucket'], item['s3_key'])

    # Include these additional columns:
    columns_to_add = ['asr_mode', 'recordingId', 'sessionId', 'class_id']

    # columns_to_remove = ['chunk_media_type', 's3_key', 's3_bucket', 'class_id', 'utterance_text', 'amr', 'asr_mode',
    #                      'recordingId', 'content_words',
    #                      'cps_code', 'cobi_key']

    for column in columns_to_add:
        if column in item:
            row[column] = item[column]

    # print(f"utterance[{utterance_id}]:'{utterance}'")
    # print(f"my_cobi_code: {my_cobi_code}")
    # print(f"row[{utterance_id}]:'{row}'")

    return row


def add_seconds_to_utc_date(utc_date_string, seconds_to_add):
    # Parse the input date string into a datetime object
    utc_datetime = dateutil.parser.isoparse(utc_date_string)

    # Add the specified number of seconds
    new_utc_datetime = utc_datetime + timedelta(milliseconds=float(seconds_to_add) * 1000)

    # Format the new datetime object into ISO format  with Z notation:
    new_utc_date_string = new_utc_datetime.isoformat().replace('+00:00', 'Z')

    return new_utc_date_string


def make_empty_utterances():
    # Provided JSON
    utterances = [{
        'amr': None,
        'confidence': None,
        'end_time': None,
        'speaker': '',
        'start_time': None,
        'text': "",
        'utterance_id': 0
    }]
    return utterances


# Write data to CSV -----
def write_cvs_file(data_source_rows, recording_id, data_sources):
    # Create a folder/directory
    # folder_name = "csv_output"
    folder_name = '/Users/emilydoherty/Desktop/JIA/raw_mmia'
    os.makedirs(folder_name, exist_ok=True)

    data_source_str = '_'.join(data_sources)
    csv_filename = os.path.join(folder_name, f'isat_data_{data_source_str}-{recording_id}.csv')

    column_headings = []
    for rows in data_source_rows:
        # Make the column headers:
        if len(rows) > 0:
            for key in rows[0].keys():
                if key not in column_headings:
                    column_headings.append(key)

    with open(csv_filename, mode='w', newline='') as csv_file:
        writer = csv.DictWriter(csv_file, fieldnames=column_headings)
        writer.writeheader()
        for rows in data_source_rows:
            for row in rows:
                # pprint.pprint(row, indent=4)
                writer.writerow(row)

    # with open(csv_filename, mode='w', newline='') as csv_file:
    #     writer = csv.DictWriter(csv_file, column_headings=items[0].keys())
    #     writer.writeheader()
    #     for item in items:
    #         writer.writerow(item)
    #         # pprint.pprint(item, indent=4)

    print(f"CSV written to file: '{csv_filename}'")


def create_console_link_for_s3_object(s3_bucket, s3_key):
    region = 'us-west-2'
    if 'aicl-media' in s3_bucket:
        region = 'us-east-1'

    return f"https://s3.console.aws.amazon.com/s3/object/{s3_bucket}?region={region}&bucketType=general&prefix={s3_key}"


def query_dynamo(recording_id, table_name):
    dyn_resource = boto3.resource('dynamodb', region_name='us-east-1')

    # print(f"loading the dynamodb {aggregate_table_name} table")
    dyn_table = dyn_resource.Table(table_name)

    # Query DynamoDB table for items with the current recording ID
    dyn_response = dyn_table.query(
        KeyConditionExpression=Key('recordingId').eq(recording_id)
    )

    return dyn_response['Items']


def query_makecode_dynamo(session_id):
    dyn_resource = boto3.resource('dynamodb', region_name='us-east-1')

    # MakeCode only has one table for both prod and dev:
    table = dyn_resource.Table("MakeCodeEvents")
    index_name = 'sessionIdIndex'

    # Perform the query operation
    response = table.query(
        IndexName=index_name,
        KeyConditionExpression=Key('sessionId').eq(session_id)
    )

    # Extract the items from the response
    return response['Items']


# The main function gets called when the script runs.
if __name__ == "__main__":
    main()
