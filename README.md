# hyperflow-viz-fileaccess

Tool for generating visualization of file block access from HyperFlow file access logs

## Usage
```
hflow-viz-fileaccess [-h|--help] [--for PLOT_FILE] [--workflow WORKFLOW_DEF] [--output OUTPUT_FILE] 
                     [--dpi DPI] [--xscale {linear,log}] [--cmap CMAP] [--blocksize BLOCK_SIZE]
                     logfile

positional arguments:
  logfile               Path to *.jsonl file with logs

optional arguments:
  -h, --help            show this help message and exit
  --for PLOT_FILE, -f PLOT_FILE
                        Path to the file the plot is generated for
  --workflow WORKFLOW_DEF, -wf WORKFLOW_DEF
                        Path to *.json file with workflow definition
  --output OUTPUT_FILE, -o OUTPUT_FILE
                        Path for the output file - used only when --for argument is set
  --dpi DPI, -d DPI     Resolution of the generated plot (DPI)
  --xscale {linear,log}, -xs {linear,log}
                        Set scale of x axis.
  --cmap CMAP, -cm CMAP
                        Set color palette. See: https://matplotlib.org/3.1.1/tutorials/colors/colormaps.html for
                        reference.
  --blocksize BLOCK_SIZE, -bs BLOCK_SIZE
                        File block size in bytes
```

## Requirements
Python 3 (tested on Python 3.8)

## Build
From the repository root:
```
pip install .
```

## Examples
Single-file mode:
```
hyperflow-viz-fileaccess -wf sample/workflow.json -f /work_dir/Gmax_275_v2.0.fa -o output_file.png -d 200 sample/file_access.jsonl
```

All-files mode:
```
hyperflow-viz-fileaccess -wf sample/workflow.json sample/file_access.jsonl
```

