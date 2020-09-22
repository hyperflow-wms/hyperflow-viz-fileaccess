# hyperflow-viz-fileaccess

Tool for generating file block access visualizations from file access logs.

## Requirements
Python 3 (tested on Python 3.8)
## Build
From the repository root:
```
pip install .
```
## Usage

```
hyperflow-viz-fileaccess -wf sample/workflow.json -f /work_dir/Gmax_275_v2.0.dict -o output_file.png -d 200 sample/file_access.jsonl
```

## Help
```
hyperflow-viz-fileaccess --help
```