# VictoriaMetrics

[![Latest Release](https://img.shields.io/github/release/VictoriaMetrics/VictoriaMetrics.svg?style=flat-square)](https://github.com/zzylol/VictoriaMetrics-cluster/releases/latest)
[![Docker Pulls](https://img.shields.io/docker/pulls/victoriametrics/victoria-metrics.svg?maxAge=604800)](https://hub.docker.com/r/victoriametrics/victoria-metrics)
[![Slack](https://img.shields.io/badge/join%20slack-%23victoriametrics-brightgreen.svg)](https://slack.victoriametrics.com/)
[![GitHub license](https://img.shields.io/github/license/VictoriaMetrics/VictoriaMetrics.svg)](https://github.com/zzylol/VictoriaMetrics-cluster/blob/master/LICENSE)
[![Go Report](https://goreportcard.com/badge/github.com/zzylol/VictoriaMetrics-cluster)](https://goreportcard.com/report/github.com/zzylol/VictoriaMetrics-cluster)
[![Build Status](https://github.com/zzylol/VictoriaMetrics-cluster/workflows/main/badge.svg)](https://github.com/zzylol/VictoriaMetrics-cluster/actions)
[![codecov](https://codecov.io/gh/VictoriaMetrics/VictoriaMetrics/branch/master/graph/badge.svg)](https://codecov.io/gh/VictoriaMetrics/VictoriaMetrics)

<picture>
  <source srcset="docs/logo_white.webp" media="(prefers-color-scheme: dark)">
  <source srcset="docs/logo.webp" media="(prefers-color-scheme: light)">
  <img src="docs/logo.webp" width="300" alt="VictoriaMetrics logo">
</picture>

VictoriaMetrics is a fast, cost-saving, and scalable solution for monitoring and managing time series data. It delivers high performance and reliability, making it an ideal choice for businesses of all sizes.

Here are some resources and information about VictoriaMetrics:

- Documentation: [docs.victoriametrics.com](https://docs.victoriametrics.com)
- Case studies: [Grammarly, Roblox, Wix,...](https://docs.victoriametrics.com/casestudies/).
- Available: [Binary releases](https://github.com/zzylol/VictoriaMetrics-cluster/releases/latest), [Docker images](https://hub.docker.com/r/victoriametrics/victoria-metrics/), [Source code](https://github.com/zzylol/VictoriaMetrics-cluster)
- Deployment types: [Single-node version](https://docs.victoriametrics.com/), [Cluster version](https://docs.victoriametrics.com/cluster-victoriametrics/), and [Enterprise version](https://docs.victoriametrics.com/enterprise/)
- Changelog: [CHANGELOG](https://docs.victoriametrics.com/changelog/), and [How to upgrade](https://docs.victoriametrics.com/#how-to-upgrade-victoriametrics)
- Community: [Slack](https://slack.victoriametrics.com/), [X (Twitter)](https://x.com/VictoriaMetrics), [LinkedIn](https://www.linkedin.com/company/victoriametrics/), [YouTube](https://www.youtube.com/@VictoriaMetrics)

Yes, we open-source both the single-node VictoriaMetrics and the cluster version.

## Prominent features

VictoriaMetrics is optimized for timeseries data, even when old time series are constantly replaced by new ones at a high rate, it offers a lot of features:

* **Long-term storage for Prometheus** or as a drop-in replacement for Prometheus and Graphite in Grafana.
* **Powerful stream aggregation**: Can be used as a StatsD alternative.
* **Ideal for big data**: Works well with large amounts of time series data from APM, Kubernetes, IoT sensors, connected cars, industrial telemetry, financial data and various [Enterprise workloads](https://docs.victoriametrics.com/enterprise/).
* **Query language**: Supports both PromQL and the more performant MetricsQL.
* **Easy to setup**: No dependencies, single [small binary](https://medium.com/@valyala/stripping-dependency-bloat-in-victoriametrics-docker-image-983fb5912b0d), configuration through command-line flags, but the default is also fine-tuned; backup and restore with [instant snapshots](https://medium.com/@valyala/how-victoriametrics-makes-instant-snapshots-for-multi-terabyte-time-series-data-e1f3fb0e0282).
* **Global query view**: Multiple Prometheus instances or any other data sources may ingest data into VictoriaMetrics and queried via a single query.
* **Various Protocols**: Support metric scraping, ingestion and backfilling in various protocol.
    * [Prometheus exporters](https://docs.victoriametrics.com/#how-to-scrape-prometheus-exporters-such-as-node-exporter), [Prometheus remote write API](https://docs.victoriametrics.com/#prometheus-setup), [Prometheus exposition format](https://docs.victoriametrics.com/#how-to-import-data-in-prometheus-exposition-format).
    * [InfluxDB line protocol](https://docs.victoriametrics.com/#how-to-send-data-from-influxdb-compatible-agents-such-as-telegraf) over HTTP, TCP and UDP.
    * [Graphite plaintext protocol](https://docs.victoriametrics.com/#how-to-send-data-from-graphite-compatible-agents-such-as-statsd) with [tags](https://graphite.readthedocs.io/en/latest/tags.html#carbon).
    * [OpenTSDB put message](https://docs.victoriametrics.com/#sending-data-via-telnet-put-protocol).
    * [HTTP OpenTSDB /api/put requests](https://docs.victoriametrics.com/#sending-opentsdb-data-via-http-apiput-requests).
    * [JSON line format](https://docs.victoriametrics.com/#how-to-import-data-in-json-line-format).
    * [Arbitrary CSV data](https://docs.victoriametrics.com/#how-to-import-csv-data).
    * [Native binary format](https://docs.victoriametrics.com/#how-to-import-data-in-native-format).
    * [DataDog agent or DogStatsD](https://docs.victoriametrics.com/#how-to-send-data-from-datadog-agent).
    * [NewRelic infrastructure agent](https://docs.victoriametrics.com/#how-to-send-data-from-newrelic-agent).
    * [OpenTelemetry metrics format](https://docs.victoriametrics.com/#sending-data-via-opentelemetry).
* **NFS-based storages**: Supports storing data on NFS-based storages such as Amazon EFS, Google Filestore.
* And many other features such as metrics relabeling, cardinality limiter, etc.

## Enterprise version

In addition, the Enterprise version includes extra features:

- **Anomaly detection**: Automation and simplification of your alerting rules, covering complex anomalies found in metrics data.
- **Backup automation**: Automates regular backup procedures.
- **Multiple retentions**: Reducing storage costs by specifying different retentions for different datasets.
- **Downsampling**: Reducing storage costs and increasing performance for queries over historical data.
- **Stable releases** with long-term support lines ([LTS](https://docs.victoriametrics.com/lts-releases/)).
- **Comprehensive support**: First-class consulting, feature requests and technical support provided by the core VictoriaMetrics dev team.
- Many other features, which you can read about on [the Enterprise page](https://docs.victoriametrics.com/enterprise/).

[Contact us](mailto:info@victoriametrics.com) if you need enterprise support for VictoriaMetrics. Or you can request a free trial license [here](https://victoriametrics.com/products/enterprise/trial/), downloaded Enterprise binaries are available at [Github Releases](https://github.com/zzylol/VictoriaMetrics-cluster/releases/latest).

We strictly apply security measures in everything we do. VictoriaMetrics has achieved security certifications for Database Software Development and Software-Based Monitoring Services. See [Security page](https://victoriametrics.com/security/) for more details.

## Benchmarks 

Some good benchmarks VictoriaMetrics achieved:

* **Minimal memory footprint**: handling millions of unique timeseries with [10x less RAM](https://medium.com/@valyala/insert-benchmarks-with-inch-influxdb-vs-victoriametrics-e31a41ae2893) than InfluxDB, up to [7x less RAM](https://valyala.medium.com/prometheus-vs-victoriametrics-benchmark-on-node-exporter-metrics-4ca29c75590f) than Prometheus, Thanos or Cortex.
* **Highly scalable and performance** for [data ingestion](https://medium.com/@valyala/high-cardinality-tsdb-benchmarks-victoriametrics-vs-timescaledb-vs-influxdb-13e6ee64dd6b) and [querying](https://medium.com/@valyala/when-size-matters-benchmarking-victoriametrics-vs-timescale-and-influxdb-6035811952d4), [20x outperforms](https://medium.com/@valyala/insert-benchmarks-with-inch-influxdb-vs-victoriametrics-e31a41ae2893) InfluxDB and TimescaleDB.
* **High data compression**: [70x more data points](https://medium.com/@valyala/when-size-matters-benchmarking-victoriametrics-vs-timescale-and-influxdb-6035811952d4) may be stored into limited storage than TimescaleDB, [7x less storage](https://valyala.medium.com/prometheus-vs-victoriametrics-benchmark-on-node-exporter-metrics-4ca29c75590f) space is required than Prometheus, Thanos or Cortex.
* **Reducing storage costs**: [10x more effective](https://docs.victoriametrics.com/casestudies/#grammarly) than Graphite according to the Grammarly case study.
* **A single-node VictoriaMetrics** can replace medium-sized clusters built with competing solutions such as Thanos, M3DB, Cortex, InfluxDB or TimescaleDB. See [VictoriaMetrics vs Thanos](https://medium.com/@valyala/comparing-thanos-to-victoriametrics-cluster-b193bea1683), [Measuring vertical scalability](https://medium.com/@valyala/measuring-vertical-scalability-for-time-series-databases-in-google-cloud-92550d78d8ae), [Remote write storage wars - PromCon 2019](https://promcon.io/2019-munich/talks/remote-write-storage-wars/).
* **Optimized for storage**: [Works well with high-latency IO](https://medium.com/@valyala/high-cardinality-tsdb-benchmarks-victoriametrics-vs-timescaledb-vs-influxdb-13e6ee64dd6b) and low IOPS (HDD and network storage in AWS, Google Cloud, Microsoft Azure, etc.).

## Community and contributions

Feel free asking any questions regarding VictoriaMetrics:

* [Slack Inviter](https://slack.victoriametrics.com/) and [Slack channel](https://victoriametrics.slack.com/)
* [X (Twitter)](https://x.com/VictoriaMetrics/)
* [Linkedin](https://www.linkedin.com/company/victoriametrics/)
* [Reddit](https://www.reddit.com/r/VictoriaMetrics/)
* [Telegram-en](https://t.me/VictoriaMetrics_en)
* [Telegram-ru](https://t.me/VictoriaMetrics_ru1)
* [Mastodon](https://mastodon.social/@victoriametrics/)

If you like VictoriaMetrics and want to contribute, then please [read these docs](https://docs.victoriametrics.com/contributing/).

## VictoriaMetrics Logo

The provided [ZIP file](https://github.com/zzylol/VictoriaMetrics-cluster/blob/master/VM_logo.zip) contains three folders with different logo orientations. Each folder includes the following file types:

* JPEG: Preview files
* PNG: Preview files with transparent background
* AI: Adobe Illustrator files

### VictoriaMetrics Logo Usage Guidelines

#### Font

* Font Used: Lato Black
* Download here: [Lato Font](https://fonts.google.com/specimen/Lato)

#### Color Palette

* Black [#000000](https://www.color-hex.com/color/000000)
* Purple [#4d0e82](https://www.color-hex.com/color/4d0e82)
* Orange [#ff2e00](https://www.color-hex.com/color/ff2e00)
* White [#ffffff](https://www.color-hex.com/color/ffffff)

### Logo Usage Rules

* Only use the Lato Black font as specified.
* Maintain sufficient clear space around the logo for visibility.
* Do not modify the spacing, alignment, or positioning of design elements.
* You may resize the logo as needed, but ensure all proportions remain intact.

Thank you for your cooperation!


# Run Docker Compose for cluster deployment
1. Build local image with changes
```
sudo make package

sudo docker image ls # list all images
```

2. Change image names and tags in docker-compose-cluster.yml after each image compilation

3. Start and Shutdown the cluster (single machine)

Start the cluster:
```
cd $VictoriaMetrics-cluster/
sudo make docker-cluster-up
```

Shutodown the cluster:
```
sudo make docker-cluster down
```


# Run Docker Compose and Docker Swarm for distributed cluster version
## Create a Swarm
Manager node:
```
sudo docker swarm init --advertise-addr 10.10.1.1
```

Worker nodes:
```
sudo  docker swarm join --token some_token 10.10.1.1:2377
```

Check status in manager node:
```
sudo docker info
sudo docker node ls
```

## Run Docker stack
Docker-compose version: >= 2.0 (install latest)

Add label meatadata to the datasource node
```
sudo docker node update --label-add role=datasource "hostname of node"
sudo docker node inspect self --pretty
```

Start stack in manager node
```
sudo docker stack deploy --compose-file deployment/docker/docker-compose-cluster-swarm.yml stackdemo
sudo docker stack deploy --compose-file deployment/docker/docker-compose-cluster-swarm-original.yml stackdemo
sudo docker stack deploy --compose-file deployment/docker/docker-compose-cluster-swarm-insert-sketches.yml stackdemo
```

Show status
```
sudo docker stack services stackdemo
```

Debug not started cluster service
```
sudo docker service ps --no-trunc {serviceName}
```

Stop stack
```
sudo  docker stack rm stackdemo
```

# Access cluster

vmalert:
```
http://hostname:8427/select/0/prometheus/vmalert/api/v1/rules
```

vmui:
```
http://hostname:8427/select/0/prometheus/vmui/
```

# Change default docker volume storage
```
mkdir /mydata/docker_volumes
vim /lib/systemd/system/docker.service
```
Edit the file as such:
```
# Old - taken from the generated docker.service file in Ubuntu 16.04's docker.io package
ExecStart=/usr/bin/dockerd -H fd:// $DOCKER_OPTS

# New
ExecStart=/usr/bin/dockerd --data-root /mydata/docker_volumes/ -H fd:// $DOCKER_OPTS
```
Restart Docker:
```
sudo systemctl daemon-reload
sudo systemctl restart docker
```

# Start each component individually in a baremetal machine (start them in order)
```
./bin/vmstorage --storageDataPath=./
./bin/vmsketch 
./bin/vmselect --storageNode=127.0.0.1:8401 --sketchNode=127.0.0.1:8501 --vmalert.proxyURL=http://127.0.0.1:8880 -search.maxQueryDuration=3600000s
./bin/vminsert --storageNode=127.0.0.1:8400 --sketchNode=127.0.0.1:8500

./bin/vmagent --promscrape.config=/mydata/VictoriaMetrics-cluster/deployment/docker/prometheus-cluster-baremetal.yml --remoteWrite.url=http://127.0.0.1:8480/insert/0/prometheus/

./bin/vmauth --auth.config=/mydata/VictoriaMetrics-cluster/deployment/docker/auth-cluster-baremetal.yml


./bin/vmalert --datasource.url=http://127.0.0.1:8427/select/0/prometheus --remoteRead.url=http://127.0.0.1:8427/select/0/prometheus --remoteWrite.url=http://127.0.0.1:8480/insert/0/prometheus  --rule=/mydata/VictoriaMetrics-cluster/deployment/docker/rules/*.yml -external.url=http://127.0.0.1:3000 -notifier.blackhole
```



# Docker images update
```
sudo docker tag victoriametrics/vminsert:[tag] zeyingz/vminsert:[tag]
sudo docker push zeyingz/vminsert:[tag]
sudo docker pull zeyingz/vminsert:[tag]
```

