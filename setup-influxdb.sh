wget https://dl.influxdata.com/influxdb/releases/influxdb_1.8.10_amd64.deb
sudo dpkg -i influxdb_1.8.10_amd64.deb

sudo systemctl start influxdb
#influxd --http-bind-address :8086 --reporting-disabled > /dev/null 2>&1 &
# until curl -s http://localhost:8086/health; do sleep 1; done
