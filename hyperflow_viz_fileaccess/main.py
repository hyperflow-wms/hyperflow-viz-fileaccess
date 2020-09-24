import jsonlines
import pandas as pd
import json
import numpy as np
import matplotlib.pyplot as plt
import argparse
from matplotlib.lines import Line2D
from datetime import datetime
import re
from matplotlib.colors import to_rgb

LOGS_DIR = "logs-hf"


def parse_log_file(log_file_path, block_size):
    data_list = []
    with jsonlines.open(log_file_path) as reader:
        for obj in reader:
            value = obj['value']
            file_path = value.get('file_path', None)
            if file_path is not None \
                    and LOGS_DIR not in file_path \
                    and obj['parameter'] == 'pread' or obj['parameter'] == 'read':
                if 'oflags' in value:
                    del value['oflags']
                del value['size']
                offset = int(value['offset'])
                value['offset'] = offset

                real_size = int(value['real_size'])
                value['real_size'] = real_size

                value['block_start_no'] = offset // block_size
                value['block_end_no'] = (offset + real_size) // block_size

                del obj['command']
                del obj['workflowId']
                del obj['parameter']
                obj['jobIdNumber'] = int(obj['jobId'].split('-')[2])
                del obj['jobId']
                data_list.append(obj)
    return pd.json_normalize(data_list)


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


def generate_plot(file_path, file_job_map, job_index_to_process, x_scale, ax, palette_name='prism'):
    print("Generate file access visualization for file: {}".format(file_path))
    jobs_num = max(job_index_to_process.keys())

    def get_min_max_blocks(data):
        start_min = float('inf')
        end_max = 0
        for access_ranges in data.values():
            for rec in access_ranges:
                if rec[0] < start_min:
                    start_min = rec[0]
                if rec[1] > end_max:
                    end_max = rec[1]
        return start_min, end_max

    def legend_elem(color_item):
        return Line2D([0], [0], lw=6, color=color_item[1], label=color_item[0])

    def get_color_palette(process_names):
        colors = plt.cm.get_cmap(palette_name, len(process_names))
        palette = {}
        for i, process_name in enumerate(process_names):
            palette[process_name] = colors(i)
        return palette

    file_data = file_job_map[file_path]
    min_block, max_block = get_min_max_blocks(file_data)
    series = np.full((jobs_num, max_block - min_block + 1, 3), 255)
    color_palette = get_color_palette(job_index_to_process.values())
    for job_id, ranges in file_data.items():
        for r in ranges:
            job_name = job_index_to_process[job_id]
            color = color_palette[job_name]
            series[job_id - 1][r[0] - min_block: r[1] - min_block + 1] = tuple(map(lambda c: c * 255, to_rgb(color)))

    ax.imshow(series,
              aspect='auto',
              interpolation='nearest',
              origin='lower')
    ax.set_title('File block access for: {}'.format(file_path), fontdict={'fontsize': 8})
    ax.grid(linewidth=0)
    if x_scale == 'log':
        ax.set_xscale("log", base=2)
    ax.set_xlim(1, max_block if max_block > 1 else 2)
    ax.set_xlabel('Block number', fontdict={'fontsize': 8})
    ax.set_ylabel('Job identifier', fontdict={'fontsize': 8})
    ax.legend(handles=[legend_elem(item) for item in color_palette.items()], loc='lower left')


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
    parser.add_argument('--xscale', '-xs',
                        dest='x_scale',
                        type=str,
                        required=False,
                        default='linear',
                        choices=['linear', 'log'],
                        help='Set scale of x axis.')
    parser.add_argument('--cmap', '-cm',
                        dest='cmap',
                        type=str,
                        required=False,
                        default='prism',
                        help='Set color palette. See: https://matplotlib.org/3.1.1/tutorials/colors/colormaps.html '
                             'for reference.')
    parser.add_argument('--blocksize', '-bs',
                        dest='block_size',
                        type=int,
                        required=False,
                        default=4096,
                        help='File block size in bytes')

    args = parser.parse_args()
    data = parse_log_file(args.logfile, args.block_size)
    job_index_to_process = parse_job_id_process_mapping(args.workflow_def)
    job_num = data['jobIdNumber'].max()
    file_job_map = get_file_job_map(data, job_num)

    plt.tight_layout()
    if args.plot_file is not None:  # Single-file mode
        output_file = args.output_file if args.output_file is not None else get_default_output_file(args.plot_file)
        generate_plot(args.plot_file,
                      file_job_map,
                      job_index_to_process,
                      args.x_scale,
                      plt.gca(),
                      palette_name=args.cmap)
        plt.savefig(output_file, dpi=args.dpi)
    else:  # All-files mode
        print(file_job_map)
        for file_name in file_job_map.keys():
            generate_plot(file_name,
                          file_job_map,
                          job_index_to_process,
                          args.x_scale,
                          plt.gca(),
                          palette_name=args.cmap)
            plt.savefig(get_default_output_file(file_name), dpi=args.dpi)


if __name__ == "__main__":
    main()
