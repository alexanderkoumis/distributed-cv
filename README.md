# DistributedCV

DistributedCV consists of three components: the MRJob-based cluster software, a web image scraper, and the front-end GUI web app. Amazon Elastic Map Reduce (EMR) jobs can be initiated to process YouTube videos or images from the web using OpenCV.

## Cluster

### Install dependencies

```bash
sudo apt-get install python-dev libpython-dev python-pip
pip install mrjob boto boto3
git clone https://github.com/alexanderkoumis/opencv-gpu-py
pip install -e ./opencv-gpu-py
```

### Usage

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

## GUI

### Install dependencies

```bash
curl --silent --location https://deb.nodesource.com/setup_4.x | sudo bash -
sudo apt-get install --yes nodejs
cd gui
npm install
```

### Usage

```bash
node app.js
# App is at http://localhost:3000
```

## Image crawler

An example [Scrapy](http://scrapy.org/) [ImagePipeline](http://doc.scrapy.org/en/latest/topics/media-pipeline.html#using-the-images-pipeline) implementation. Recursively visits all valid links on a page, downloading all images to `imgscrape/output`.

### Install dependencies
```bash
pip install scrapy validators
```

### Usage
```
cd scraper
scrapy crawl img
```
