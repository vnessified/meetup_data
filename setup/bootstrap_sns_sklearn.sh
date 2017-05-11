# bootstrap script with more speciality packages
sudo yum install -y python27-pip python27-devel
sudo pip install -U pip
sudo python -m pip install pip pyyaml ipython jupyter pandas seaborn scikit-learn boto beautifulsoup4 -U
