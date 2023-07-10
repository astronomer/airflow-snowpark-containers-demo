FROM grafana/grafana:latest

COPY dashboard.yml /etc/grafana/provisioning/dashboards/local.yml
COPY datasources.yml /etc/grafana/provisioning/datasources/automatic.yml
COPY AirflowLogo.png /usr/share/grafana/public/img/AirflowLogo.png
COPY dashboard.json /var/lib/grafana/dashboards/dashboard.json