#!/bin/sh
ip="127.0.0.1"
ETCD_CLIENT_PORT="12379"


while read -u 31 ip; do

  # echo -e "\t.. Checking replica on ${ip}";

  wget "http://${ip}:${ETCD_CLIENT_PORT}/v2/stats/leader" --tries=2 --timeout=5 -O /dev/null --server-response -q 2>&1 | grep '200 OK' >/dev/null
  if [[ $? -eq 0 ]]; then
      echo "${ip}"
      exit 0;
  fi
  echo "${ETCD_CLIENT_PORT} is not the leader ($?)"

done 31<${input};
