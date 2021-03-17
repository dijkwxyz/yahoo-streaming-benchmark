sudo wget http://repos.fedorapeople.org/repos/dchen/apache-maven/epel-apache-maven.repo -O /etc/yum.repos.d/epel-apache-maven.repo
# will install java 1.8
sudo yum -y install apache-maven

#install gcc
sudo yum -y install gcc

#setup benchmark environment
./stream-bench.sh INSTALL


#install lein
sudo wget https://raw.githubusercontent.com/technomancy/leiningen/stable/bin/lein -P ~/bin
sudo chmod a+x ~/bin/lein
lein
