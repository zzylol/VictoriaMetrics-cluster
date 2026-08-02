# VictoriaMetrics

[![Última versión](https://img.shields.io/github/release/VictoriaMetrics/VictoriaMetrics.svg?style=flat-square)](https://github.com/zzylol/VictoriaMetrics-cluster/releases/latest)
[![Pulsaciones de Docker](https://img.shields.io/docker/pulls/victoriametrics/victoria-metrics.svg?maxAge=604800)](https://hub.docker.com/r/victoriametrics/victoria-metrics)
[![Slack](https://img.shields.io/badge/join%20slack-%23victoriametrics-brightgreen.svg)](https://slack.victoriametrics.com/)
[![Licencia GitHub](https://img.shields.io/github/license/VictoriaMetrics/VictoriaMetrics.svg)](https://github.com/zzylol/VictoriaMetrics-cluster/blob/master/LICENSE)
[![Informe Go](https://goreportcard.com/badge/github.com/zzylol/VictoriaMetrics-cluster)](https://goreportcard.com/report/github.com/zzylol/VictoriaMetrics-cluster)
[![Estado de compilación](https://github.com/zzylol/VictoriaMetrics-cluster/workflows/main/badge.svg)](https://github.com/zzylol/VictoriaMetrics-cluster/actions)
[![codecov](https://codecov.io/gh/VictoriaMetrics/VictoriaMetrics/branch/master/graph/badge.svg)](https://codecov.io/gh/VictoriaMetrics/VictoriaMetrics)

<picture>
  <source srcset="docs/logo_white.webp" media="(prefers-color-scheme: dark)">
  <source srcset="docs/logo.webp" media="(prefers-color-scheme: light)">
  <img src="docs/logo.webp" width="300" alt="VictoriaMetrics logo">
</picture>

VictoriaMetrics es una solución rápida, económica y escalable para el monitoreo y la gestión de datos de series temporales. Ofrece un alto rendimiento y fiabilidad, lo que la convierte en una opción ideal para empresas de todos los tamaños.

Aquí hay algunos recursos e información sobre VictoriaMetrics:

- Documentación: [docs.victoriametrics.com](https://docs.victoriametrics.com)
- Casos de estudio: [Grammarly, Roblox, Wix,...](https://docs.victoriametrics.com/casestudies/).
- Disponible: [Versiones binarias](https://github.com/zzylol/VictoriaMetrics-cluster/releases/latest), [Imágenes Docker](https://hub.docker.com/r/victoriametrics/victoria-metrics/), [Código fuente](https://github.com/zzylol/VictoriaMetrics-cluster)
- Tipos de despliegue: [Versión de nodo único](https://docs.victoriametrics.com/), [Versión en clúster](https://docs.victoriametrics.com/cluster-victoriametrics/), y [Versión Enterprise](https://docs.victoriametrics.com/enterprise/)
- Registro de cambios: [CHANGELOG](https://docs.victoriametrics.com/changelog/), y [Cómo actualizar](https://docs.victoriametrics.com/#how-to-upgrade-victoriametrics)
- Comunidad: [Slack](https://slack.victoriametrics.com/), [X (Twitter)](https://x.com/VictoriaMetrics), [LinkedIn](https://www.linkedin.com/company/victoriametrics/), [YouTube](https://www.youtube.com/@VictoriaMetrics)

Sí, somos de código abierto tanto la versión de nodo único de VictoriaMetrics como la versión en clúster.

## Características destacadas

VictoriaMetrics está optimizada para datos de series temporales, incluso cuando las series temporales antiguas son constantemente reemplazadas por nuevas a un ritmo alto, ofrece muchas funciones:

* **Almacenamiento a largo plazo para Prometheus** o como reemplazo directo de Prometheus y Graphite en Grafana.
* **Poderosa agregación de flujos**: Puede usarse como alternativa a StatsD.
* **Ideal para big data**: Funciona bien con grandes cantidades de datos de series temporales de APM, Kubernetes, sensores IoT, coches conectados, telemetría industrial, datos financieros y diversas [cargas de trabajo empresariales](https://docs.victoriametrics.com/enterprise/).
* **Lenguaje de consulta**: Compatible tanto con PromQL como con la más performante MetricsQL.
* **Fácil de configurar**: Sin dependencias, un único [pequeño binario](https://medium.com/@valyala/stripping-dependency-bloat-in-victoriametrics-docker-image-983fb5912b0d), configuración mediante banderas de línea de comandos, pero el valor por defecto también está bien ajustado; copia de seguridad y restauración con [instantáneas instantáneas](https://medium.com/@valyala/how-victoriametrics-makes-instant-snapshots-for-multi-terabyte-time-series-data-e1f3fb0e0282).
* **Vista de consulta global**: Múltiples instancias de Prometheus o cualquier otra fuente de datos pueden ingerir datos en VictoriaMetrics y consultarse a través de una sola consulta.
* **Varios protocolos**: Compatible con la recopilación de métricas, ingesta y retroalimentación en varios protocolos.
    * [Exportadores de Prometheus](https://docs.victoriametrics.com/#how-to-scrape-prometheus-exporters-such-as-node-exporter), [API de escritura remota de Prometheus](https://docs.victoriametrics.com/#prometheus-setup), [formato de exposición de Prometheus](https://docs.victoriametrics.com/#how-to-import-data-in-prometheus-exposition-format).
    * [Protocolo de línea de InfluxDB](https://docs.victoriametrics.com/#how-to-send-data-from-influxdb-compatible-agents-such-as-telegraf) sobre HTTP, TCP y UDP.
    * [Protocolo de texto plano de Graphite](https://docs.victoriametrics.com/#how-to-send-data-from-graphite-compatible-agents-such-as-statsd) con [etiquetas](https://graphite.readthedocs.io/en/latest/tags.html#carbon).
    * [Mensaje put de OpenTSDB](https://docs.victoriametrics.com/#sending-data-via-telnet-put-protocol).
    * [Solicitudes HTTP OpenTSDB /api/put](https://docs.victoriametrics.com/#sending-opentsdb-data-via-http-apiput-requests).
    * [Formato de línea JSON](https://docs.victoriametrics.com/#how-to-import-data-in-json-line-format).
    * [Datos CSV arbitrarios](https://docs.victoriametrics.com/#how-to-import-csv-data).
    * [Formato binario nativo](https://docs.victoriametrics.com/#how-to-import-data-in-native-format).
    * [Agente DataDog o DogStatsD](https://docs.victoriametrics.com/#how-to-send-data-from-datadog-agent).
    * [Agente de infraestructura NewRelic](https://docs.victoriametrics.com/#how-to-send-data-from-newrelic-agent).
    * [Formato de métricas OpenTelemetry](https://docs.victoriametrics.com/#sending-data-via-opentelemetry).
* **Almacenamientos basados en NFS**: Compatible con el almacenamiento de datos en almacenamientos basados en NFS como Amazon EFS, Google Filestore.
* Y muchas otras funciones como reetiquetado de métricas, limitador de cardinalidad, etc.

## Versión Enterprise

Además, la versión Enterprise incluye funciones adicionales:

- **Detección de anomalías**: Automatización y simplificación de las reglas de alerta, cubriendo anomalías complejas encontradas en los datos de métricas.
- **Automatización de copias de seguridad**: Automatiza los procedimientos regulares de copia de seguridad.
- **Retenciones múltiples**: Reduce los costos de almacenamiento especificando diferentes retenciones para diferentes conjuntos de datos.
- **Submuestreo**: Reduce los costos de almacenamiento y aumenta el rendimiento para consultas sobre datos históricos.
- **Versiones estables** con líneas de soporte a largo plazo ([LTS](https://docs.victoriametrics.com/lts-releases/)).
- **Soporte integral**: Consultoría de primera clase, solicitudes de funciones y soporte técnico proporcionados por el equipo de desarrollo principal de VictoriaMetrics.
- Muchas otras funciones, que puedes leer en la [página Enterprise](https://docs.victoriametrics.com/enterprise/).

[Contáctanos](mailto:info@victoriametrics.com) si necesitas soporte empresarial para VictoriaMetrics. O puedes solicitar una licencia de prueba gratuita [aquí](https://victoriametrics.com/products/enterprise/trial/), las binarios Enterprise descargados están disponibles en [Github Releases](https://github.com/zzylol/VictoriaMetrics-cluster/releases/latest).

Aplicamos estrictamente medidas de seguridad en todo lo que hacemos. VictoriaMetrics ha logrado certificaciones de seguridad para Desarrollo de Software de Bases de Datos y Servicios de Monitoreo Basados en Software. Consulta la [página de seguridad](https://victoriametrics.com/security/) para más detalles.

## Pruebas comparativas (Benchmarks)

Algunas buenas pruebas comparativas que VictoriaMetrics ha logrado:

* **Mínimo consumo de memoria**: manejo de millones de series temporales únicas con [10 veces menos RAM](https://medium.com/@valyala/insert-benchmarks-with-inch-influxdb-vs-victoriametrics-e31a41ae2893) que InfluxDB, hasta [7 veces menos RAM](https://valyala.medium.com/prometheus-vs-victoriametrics-benchmark-on-node-exporter-metrics-4ca29c75590f) que Prometheus, Thanos o Cortex.
* **Altamente escalable y performante** para [ingesta de datos](https://medium.com/@valyala/high-cardinality-tsdb-benchmarks-victoriametrics-vs-timescaledb-vs-influxdb-13e6ee64dd6b) y [consultas](https://medium.com/@valyala/when-size-matters-benchmarking-victoriametrics-vs-timescale-and-influxdb-6035811952d4), [20 veces supera](https://medium.com/@valyala/insert-benchmarks-with-inch-influxdb-vs-victoriametrics-e31a41ae2893) a InfluxDB y TimescaleDB.
* **Alta compresión de datos**: [70 veces más puntos de datos](https://medium.com/@valyala/when-size-matters-benchmarking-victoriametrics-vs-timescale-and-influxdb-6035811952d4) pueden almacenarse en un almacenamiento limitado que TimescaleDB, se requiere [7 veces menos espacio de almacenamiento](https://valyala.medium.com/prometheus-vs-victoriametrics-benchmark-on-node-exporter-metrics-4ca29c75590f) que Prometheus, Thanos o Cortex.
* **Reducción de costos de almacenamiento**: [10 veces más eficaz](https://docs.victoriametrics.com/casestudies/#grammarly) que Graphite según el caso de estudio de Grammarly.
* **Una VictoriaMetrics de nodo único** puede reemplazar clústeres de tamaño medio construidos con soluciones competidoras como Thanos, M3DB, Cortex, InfluxDB o TimescaleDB. Consulta [VictoriaMetrics vs Thanos](https://medium.com/@valyala/comparing-thanos-to-victoriametrics-cluster-b193bea1683), [Medición de escalabilidad vertical](https://medium.com/@valyala/measuring-vertical-scalability-for-time-series-databases-in-google-cloud-92550d78d8ae), [Guerras de almacenamiento de escritura remota - PromCon 2019](https://promcon.io/2019-munich/talks/remote-write-storage-wars/).
* **Optimizado para almacenamiento**: [Funciona bien con E/S de alta latencia](https://medium.com/@valyala/high-cardinality-tsdb-benchmarks-victoriametrics-vs-timescaledb-vs-influxdb-13e6ee64dd6b) y baja IOPS (almacenamiento HDD y de red en AWS, Google Cloud, Microsoft Azure, etc.).

## Comunidad y contribuciones

Siéntete libre de hacer cualquier pregunta sobre VictoriaMetrics:

* [Invitador a Slack](https://slack.victoriametrics.com/) y [canal de Slack](https://victoriametrics.slack.com/)
* [X (Twitter)](https://x.com/VictoriaMetrics/)
* [LinkedIn](https://www.linkedin.com/company/victoriametrics/)
* [Reddit](https://www.reddit.com/r/VictoriaMetrics/)
* [Telegram-en](https://t.me/VictoriaMetrics_en)
* [Telegram-ru](https://t.me/VictoriaMetrics_ru1)
- [Mastodon](https://mastodon.social/@victoriametrics/)

Si te gusta VictoriaMetrics y quieres contribuir, por favor [lee estos documentos](https://docs.victoriametrics.com/contributing/).

## Logo de VictoriaMetrics

El [archivo ZIP](https://github.com/zzylol/VictoriaMetrics-cluster/blob/master/VM_logo.zip) proporcionado contiene tres carpetas con diferentes orientaciones de logo. Cada carpeta incluye los siguientes tipos de archivo:

* JPEG: Archivos de vista previa
* PNG: Archivos de vista previa con fondo transparente
* AI: Archivos de Adobe Illustrator

### Guías de uso del logo de VictoriaMetrics

#### Fuente

* Fuente utilizada: Lato Black
* Descárgala aquí: [Fuente Lato](https://fonts.google.com/specimen/Lato)

#### Paleta de colores

* Negro [#000000](https://www.color-hex.com/color/000000)
* Púrpura [#4d0e82](https://www.color-hex.com/color/4d0e82)
* Naranja [#ff2e00](https://www.color-hex.com/color/ff2e00)
* Blanco [#ffffff](https://www.color-hex.com/color/ffffff)

### Reglas de uso del logo

* Usa solo la fuente Lato Black como se especifica.
* Mantén un espacio claro suficiente alrededor del logo para garantizar su visibilidad.
* No modifiques el espaciado, la alineación o la posición de los elementos de diseño.
* Puedes redimensionar el logo según sea necesario, pero asegúrate de que todas las proporciones se mantengan intactas.

¡Gracias por tu cooperación!


# Ejecutar Docker Compose para despliegue en clúster
1. Construir imagen local con cambios
```
sudo make package

sudo docker image ls # listar todas las imágenes
```

2. Cambiar los nombres y etiquetas de las imágenes en docker-compose-cluster.yml después de cada compilación de imagen

3. Iniciar y apagar el clúster (máquina única)

Iniciar el clúster:
```
cd $VictoriaMetrics-cluster/
sudo make docker-cluster-up
```

Apagar el clúster:
```
sudo make docker-cluster down
```


# Ejecutar Docker Compose y Docker Swarm para la versión de clúster distribuida
## Crear un Swarm
Nodo gestor:
```
sudo docker swarm init --advertise-addr 10.10.1.1
```

Nodos trabajadores:
```
sudo  docker swarm join --token some_token 10.10.1.1:2377
```

Verificar estado en el nodo gestor:
```
sudo docker info
sudo docker node ls
```

## Ejecutar Docker stack
Versión de docker-compose: >= 2.0 (instalar la última)

Añadir metadatos de etiqueta al nodo de fuente de datos
```
sudo docker node update --label-add role=datasource "hostname of node"
sudo docker node inspect self --pretty
```

Iniciar stack en el nodo gestor
```
sudo docker stack deploy --compose-file deployment/docker/docker-compose-cluster-swarm.yml stackdemo
sudo docker stack deploy --compose-file deployment/docker/docker-compose-cluster-swarm-original.yml stackdemo
```

Mostrar estado
```
sudo docker stack services stackdemo
```

Depurar servicio de clúster no iniciado
```
sudo docker service ps --no-trunc {serviceName}
sudo docker service logs {serviceName} # mostrar los registros 
```

Detener stack
```
sudo  docker stack rm stackdemo
```

# Acceder al clúster

vmalert:
```
http://hostname:8427/select/0/prometheus/vmalert/api/v1/rules
```

vmui:
```
http://hostname:8427/select/0/prometheus/vmui/
```

# Cambiar el almacenamiento de volumen de Docker predeterminado
```
mkdir /mydata/docker_volumes
vim /lib/systemd/system/docker.service
```
Editar el archivo así:
```
# Antiguo - tomado del archivo docker.service generado en el paquete docker.io de Ubuntu 16.04
ExecStart=/usr/bin/dockerd -H fd:// $DOCKER_OPTS

# Nuevo
ExecStart=/usr/bin/dockerd --data-root /mydata/docker_volumes/ -H fd:// $DOCKER_OPTS
```
Reiniciar Docker:
```
sudo systemctl daemon-reload
sudo systemctl restart docker
```

# Iniciar cada componente individualmente en una máquina baremetal (inícialos en orden)
```
./bin/vmstorage --storageDataPath=./
./bin/vmsketch 
./bin/vmselect --storageNode=127.0.0.1:8401 --sketchNode=127.0.0.1:8501 --vmalert.proxyURL=http://127.0.0.1:8880 -search.maxQueryDuration=3600000s
./bin/vminsert --storageNode=127.0.0.1:8400 --sketchNode=127.0.0.1:8500

./bin/vmagent --promscrape.config=/mydata/VictoriaMetrics-cluster/deployment/docker/prometheus-cluster-baremetal.yml --remoteWrite.url=http://127.0.0.1:8480/insert/0/prometheus/

./bin/vmauth --auth.config=/mydata/VictoriaMetrics-cluster/deployment/docker/auth-cluster-baremetal.yml


./bin/vmalert --datasource.url=http://127.0.0.1:8427/select/0/prometheus --remoteRead.url=http://127.0.0.1:8427/select/0/prometheus --remoteWrite.url=http://127.0.0.1:8480/insert/0/prometheus  --rule=/mydata/VictoriaMetrics-cluster/deployment/docker/rules/*.yml -external.url=http://127.0.0.1:3000 -notifier.blackhole
```


# Actualización de imágenes Docker
```
sudo docker tag victoriametrics/vminsert:[tag] zeyingz/vminsert:[tag]
sudo docker push zeyingz/vminsert:[tag]
sudo docker pull zeyingz/vminsert:[tag]
```
