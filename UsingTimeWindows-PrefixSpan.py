import pandas as pd
import re
from prefixspan import PrefixSpan

input_path = "DataFiles"
output_path = "OutputFiles/FixedWindowByTime"

def replace_variable_numbers(text):
    return re.sub(r"\b\d+\b", "<NUM>", text)


def parse_logs(file_path):
    log_pattern = re.compile(
        # r"\[(?P<timestamp>[\d\-:\.,\s]+)\] \[(?P<log_level>\w+)\s*\] \[(?P<group_id>\d+)\]\[(?P<thread_id>\d+)\]\[(?P<module>[\w/]+)\] - (?P<message>.+)"
        r"\[(?P<timestamp>[\d\-:\.,\s]+)\] \[(?P<log_level>\w+)\s*\] \[(?P<group_id>\d+)\]\[(?P<thread_id>\d+)\]\[(?P<module>[\w./:-]+)\] - (?P<message>.+)"
    )

    logs = []
    with open(file_path, 'r') as file:
        for line in file:
            match = log_pattern.match(line.strip())
            if match:
                logs.append(match.groupdict())
    # pd.DataFrame(logs).to_csv(output_path + "/logs_in_csv_"+file_path.replace(".log","")+".csv", index=False)
    pd.DataFrame(logs).to_csv(output_path + "/logs_in_csv.csv", index=False)
    return pd.DataFrame(logs)

def split_data_according_to_timestamp(df):

    df['timestamp'] = pd.to_datetime(df['timestamp'], format='%Y-%m-%d %H:%M:%S,%f')
    df['time_group'] = df['timestamp'].dt.strftime('%Y-%m-%d %H:%M')
    # df['module_message'] = df['module'] + " - " + df['message']
    df["module_message"] = df["module"].apply(replace_variable_numbers) + " - " + df["message"].apply(replace_variable_numbers)

    # minute_groups = df.groupby('time_group')['message'].apply(list).tolist()

    minute_groups = df.groupby('time_group')['module_message'].apply(list).tolist()
    return minute_groups


def prefix_apply(minute_groups,min_sequence_length,max_sequence_length,frequency_needed):
    ps = PrefixSpan(minute_groups)
    ps.minlen = min_sequence_length
    ps.maxlen = max_sequence_length
    frequent_sequences = ps.frequent(frequency_needed) 

    frequent_itemsets = pd.DataFrame(frequent_sequences, columns=["support", "itemsets"])

    frequent_itemsets = frequent_itemsets.sort_values(by="support", ascending=False).reset_index(drop=True)
    frequent_itemsets.to_csv(output_path + "/frequent_itemsets.csv")
    return frequent_itemsets

def extract_largest_groups(frequent_itemsets):

    largest_group = {}
    for _, row in frequent_itemsets.iterrows():
        items = tuple(row['itemsets'])  # Convert to tuple to maintain order
        for item in items:
            if item not in largest_group or len(largest_group[item]) < len(items):
                largest_group[item] = items  # Update if a larger sequence is found

    largest_group_df = pd.DataFrame(
        [(item, list(group), len(group)) for item, group in largest_group.items()],
        columns=["Log Message", "Sequenced Largest Group", "Group Size"]
    )
    largest_group_df.to_csv(output_path + "/largest_group_sequenced.csv", index=False)
    return largest_group_df


def retrieve_unique_groups(largest_group_df):
    unique_groups = {tuple(group) for group in largest_group_df["Sequenced Largest Group"]}
    # unique_groups_text = "\n".join(
    #     [f"Group {i+1} (Size: {len(group)}): {', '.join(map(str, group))}\n" + "-" * 90
    #     for i, group in enumerate(unique_groups)]
    # )
    unique_groups_text = "\n".join(
    [f"Group {i+1} (Group Size: {len(group)}):\n" + "\n".join(map(str, group)) + "\n" + "-" * 90
     for i, group in enumerate(unique_groups)]
    )
    with open(output_path + "/unique_groups.txt", "w") as f:
        f.write(unique_groups_text)



df = parse_logs(input_path + '/api_first500.txt')

minute_groups = split_data_according_to_timestamp(df)

for group in minute_groups:
    print(group)


min_sequence_length = 2  
max_sequence_length = 30
# frequency_needed_for_a_group_to_be_considered_frequent
frequency_needed = 3 

frequent_itemsets = prefix_apply(minute_groups,min_sequence_length,max_sequence_length,frequency_needed)
largest_group_df = extract_largest_groups(frequent_itemsets)
retrieve_unique_groups(largest_group_df)


