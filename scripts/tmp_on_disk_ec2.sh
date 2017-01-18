#!/bin/bash

# This puts your tmp on disk on the EC2 installation, so that
# you have enough space. Normally /tmp is memory based and runs out.

if [ $(whoami) != 'root' ]; then
  echo 'You need to run this with sudo. Run: sudo scripts/tmp_on_disk_ec2.sh'
  exit 1
fi

if [ ! -d /home/dev/ ]; then
  echo 'No /home/dev/. Is this an EC2 box?'
  exit 1
fi

mkdir -p /home/dev/tmp
chmod 1777 /home/dev/tmp
sudo mv /tmp/* /home/dev/tmp
sudo rm -r /tmp
sudo ln -s /home/dev/tmp /tmp
