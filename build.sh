#sudo docker run --hostname=quickstart01.gsort --privileged=true -t -i -p=8888 cloudera/quickstart /usr/bin/docker-quickstart

#sudo docker build -t quickstart_gsort .
#sudo docker run --hostname=node1 --privileged=true -d -p 8888 -it quickstart_gsort /usr/bin/docker-quickstart
#sudo docker run --hostname=node2 --privileged=true  -it quickstart_gsort /usr/bin/docker-quickstart
#sudo docker run --hostname=node3 --privileged=true  -it quickstart_gsort /usr/bin/docker-quickstart
su
#VIRTUALBOX
echo "deb http://download.virtualbox.org/virtualbox/debian trusty contrib" >> /etc/apt/sources.list
wget https://www.virtualbox.org/download/oracle_vbox.asc
apt-key add oracle_vbox.asc
apt-get update
apt-get install virtualbox-5.0 dkms -y

#VAGRANT
wget https://releases.hashicorp.com/vagrant/1.8.1/vagrant_1.8.1_x86_64.deb
dpkg -i vagrant_1.8.1_x86_64.deb
vagrant box add centos65 http://files.brianbirkinbine.com/vagrant-centos-65-i386-minimal.box

#GIT
git clone https://github.com/dnafrance/vagrant-hadoop-spark-cluster.git

cd vagrant-hadoop-spark-cluster/resources
#wget https://archive.apache.org/dist/hadoop/core/hadoop-2.6.0/hadoop-2.6.0.tar.gz
wget http://mirror.nbtelecom.com.br/apache/hadoop/common/hadoop-2.7.2/hadoop-2.7.2.tar.gz
wget http://d3kbcqa49mib13.cloudfront.net/spark-1.1.1-bin-hadoop2.4.tgz
wget http://download.oracle.com/otn-pub/java/jdk/8u25-b17/jdk-8u25-linux-i586.tar.gz


vagrant up
vagrant ssh 
vagrant destroy 