# DistributedCV - Cluster

## Install dependencies

```bash
sudo apt-get install python-dev libpython-dev python-pip
pip install mrjob boto boto3
git clone https://github.com/alexanderkoumis/opencv-gpu-py
pip install -e ./opencv-gpu-py
```

## Usage

Running cluster:
```bash
cluster.py [-h] [--cpu | --gpu]
                [--youtube YOUTUBE | --directory DIRECTORY | --archive ARCHIVE]
                [--list LIST] [--run RUN_TYPE]
                [--num_instances NUM_INSTANCES] [--verbose]

optional arguments:
  -h, --help            show this help message and exit
  --cpu                 use CPU face detector
  --gpu                 enable GPU (Costs more with AWS)
  --youtube YOUTUBE     input YouTube video (requires youtube-dl)
  --directory DIRECTORY
                        input directory of images
  --archive ARCHIVE     input archive (must also supply list)
  --list LIST           input list
  --run RUN_TYPE        ( simulate | local | emr )
  --num_instances NUM_INSTANCES
                        number of EC2 instances
  --verbose             print out debug information
```

Running the tests with: `cluster_test.py`