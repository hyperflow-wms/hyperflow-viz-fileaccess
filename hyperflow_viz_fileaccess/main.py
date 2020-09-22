import jsonlines
import pandas as pd
import json
import numpy as np
import matplotlib.pyplot as plt
import argparse
from matplotlib.lines import Line2D
from datetime import datetime
import re

# File block size (in bytes)
BLOCK_SIZE = 4096

LOGS_DIR = "logs-hf"

WORKFLOW_DEF = "sample/workflow.json"

color_palette = {
    'genotype_gvcfs': (90, 88, 149),
    'merge_gcvf': (139, 161, 188),
    'haplotype_caller': (53, 131, 89),
    'indel_realign': (0, 126, 127),
    'realign_target_creator': (138, 184, 207),
    'combine_variants': (112, 140, 152),
    'select_variants_snp': (149, 130, 141),
    'filtering_snp': (117, 180, 30),
    'select_variants_indel': (240, 136, 146),
    'filtering_indel': (1, 106, 64)
}


def parse_log_file(log_file_path, block_size):
    def read_file():
        data_list = []
        with jsonlines.open(log_file_path) as reader:
            for obj in reader:
                data_list.append(obj)
        return data_list

    data = pd.json_normalize(read_file())
    data = data.drop('value.oflags', axis=1)
    data = data.drop('value.size', axis=1)
    data = data.drop('command', axis=1)
    data = data.drop('workflowId', axis=1)
    data['jobIdNumber'] = data['jobId'].apply(lambda jobId: int(jobId.split('-')[2]))
    data = data.drop('jobId', axis=1)
    data = data[~data['value.file_path'].str.contains(LOGS_DIR)]
    data = data[(data['parameter'] == 'read') | (data['parameter'] == 'pread')]
    data = data.drop('parameter', axis=1)
    data['value.offset'] = data['value.offset'].astype('int32')
    data['value.real_size'] = data['value.real_size'].astype('int32')
    data['value.block_start_no'] = data['value.offset'] // block_size
    data['value.block_end_no'] = (data['value.offset'] + data['value.real_size']) // block_size
    return data


def parse_job_id_process_mapping(workflow_def_path):
    def open_wf_def():
        with open(workflow_def_path) as json_file:
            return json.load(json_file)['processes']

    wf_def = open_wf_def()
    job_index_to_process = {}
    for idx, task in enumerate(wf_def):
        job_index_to_process[idx + 1] = task['name']
    return job_index_to_process


def reduce_records(t1, t2):
    t1_start, t1_end = t1
    t2_start, t2_end = t2
    if t1_start == t2_start:
        return [(t1_start, max(t1_end, t2_end))]

    if t1_end >= t2_start:
        return [(t1_start, t2_end)]

    return [t1, t2]


def files_by_job_id(dataset, job_id):
    filtered = dataset[dataset['jobIdNumber'] == job_id]
    file_reads = {}
    for idx, row in filtered.iterrows():
        file_reads.setdefault(row['value.file_path'], []).append(
            (row['value.block_start_no'], row['value.block_end_no']))

    processed_dict = {}
    for key, value in file_reads.items():
        value_sorted = sorted(value, key=lambda tup: tup[0])
        value_reduced = [value_sorted[0]]
        reduced_idx = 0
        for elem in value_sorted[1:]:
            reduced = reduce_records(value_reduced[reduced_idx], elem)
            reduced_len = len(reduced)
            if reduced_len == 2:
                value_reduced[reduced_idx] = reduced[0]
                value_reduced.append(reduced[1])
                reduced_idx = reduced_idx + 1
            elif reduced_len == 1:
                value_reduced[reduced_idx] = reduced[0]
        processed_dict[key] = value_reduced

    return processed_dict


def get_file_job_map(dataset, jobs_num):
    files_by_job = {i: files_by_job_id(dataset, i) for i in range(1, jobs_num + 1)}
    file_job_map = {}
    for j_id, file_map in files_by_job.items():
        for filename, access_ranges in file_map.items():
            file_job_map.setdefault(filename, {}).setdefault(j_id, []).extend(access_ranges)
    return file_job_map


def generate_plot(file_path, file_job_map, job_index_to_process, ax):
    print("Generate file access visualization for file: {}".format(file_path))
    jobs_num = max(job_index_to_process.keys())

    def get_min_max_blocks(data):
        all_starting = []
        all_ending = []
        for j_id, access_ranges in data.items():
            for r in access_ranges:
                all_starting.append(r[0])
                all_ending.append(r[1])
        return min(all_starting), max(all_ending)

    def legend_elem(color_item):
        return Line2D([0], [0], lw=6, color=tuple(map(lambda x: x / 255, color_item[1])), label=color_item[0])

    file_data = file_job_map[file_path]
    min_block, max_block = get_min_max_blocks(file_data)
    series = np.full((jobs_num, max_block - min_block + 1, 3), 255)
    colors_palette_used = {}
    for job_id, ranges in file_data.items():
        for r in ranges:
            job_name = job_index_to_process[job_id]
            color = color_palette[job_name]
            colors_palette_used[job_name] = color
            series[job_id - 1][r[0] - min_block: r[1] - min_block + 1] = color

    ax.imshow(series,
              aspect='auto',
              interpolation='nearest',
              origin='lower')
    ax.set_title('File block access for: {}'.format(file_path),  fontdict={'fontsize': 8})
    ax.grid(linewidth=0)
    ax.set_xscale("log", base=2)
    ax.set_xlim(1, max_block if max_block > 1 else 2)
    ax.set_xlabel('Block number', fontdict={'fontsize': 8})
    ax.set_ylabel('Job identifier', fontdict={'fontsize': 8})
    ax.legend(handles=[legend_elem(item) for item in colors_palette_used.items()], loc='lower left')


def get_time_prefix():
    current_time = datetime.now()
    return current_time.strftime("%Y%m%d%H%M%S")


def get_default_output_file(file_path):
    return re.sub(r'[^A-Za-z0-9]+', '', file_path) + "_" + get_time_prefix() + ".png"


def main():
    parser = argparse.ArgumentParser(description='Visualize file block access', allow_abbrev=False)
    parser.add_argument('logfile',
                        type=str,
                        help='Path to *.jsonl file with logs')
    parser.add_argument('--for', '-f',
                        dest='plot_file',
                        type=str,
                        help='Path to the file the plot is generated for')
    parser.add_argument('--workflow', '-wf',
                        dest='workflow_def',
                        type=str,
                        default="workflow.json",
                        help='Path to *.json file with workflow definition')
    parser.add_argument('--output', '-o',
                        dest='output_file',
                        type=str,
                        required=False,
                        help='Path for the output file - used only when --for argument is set')
    parser.add_argument('--dpi', '-d',
                        dest='dpi',
                        type=int,
                        required=False,
                        default=150,
                        help='Resolution of the generated plot (DPI)')

    args = parser.parse_args()
    data = parse_log_file(args.logfile, BLOCK_SIZE)
    job_index_to_process = parse_job_id_process_mapping(args.workflow_def)
    job_num = data['jobIdNumber'].max()
    file_job_map = get_file_job_map(data, job_num)

    plt.tight_layout()
    if args.plot_file is not None:  # Single-file mode
        output_file = args.output_file if args.output_file is not None else get_default_output_file(args.plot_file)
        generate_plot(args.plot_file, file_job_map, job_index_to_process, plt.gca())
        plt.savefig(output_file, dpi=args.dpi)
    else:  # All-files mode
        print(file_job_map)
        for file_name in file_job_map.keys():
            generate_plot(file_name, file_job_map, job_index_to_process, plt.gca())
            plt.savefig(get_default_output_file(file_name), dpi=args.dpi)


if __name__ == "__main__":
    main()
