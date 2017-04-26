sudo apt-get -y update
sudo apt install -y python3-pip
python3 -m pip install pip awscli boto3 requests jsonlines -U


sudo apt-get install -y apache2
if ! [ -L /var/www ]; then
  rm -rf /var/www
  ln -fs /vagrant /var/www
fi
