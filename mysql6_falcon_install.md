# Compile MySQL From Source #
  * configure options http://dev.mysql.com/doc/refman/5.4/en/configure-options.html
  * http://dev.mysql.com/doc/refman/5.4/en/quick-install.html

# Development Cycle Has Changed #
> The development cycle has changed so I am trying 5.4 out. My main need is the subquery optimization, which I think is fixed in 5.1+. So I am trying 5.4 out.

# MySql install notes on Ubuntu 9.04 #
> If you use this, you may have to read in between the lines here and there. I wasn't able to get the icc already compiled version to work b/c of a syntax error with my\_print\_defaults. But I compiled it from source and its working perfect. The sub-query system is so much faster, its unbelievable. I recommend using the mysql6-falcon alpha linux source install.

> Compile Mysql6-falcon for ubuntu 9.04.
```
# http://www.howtoforge.com/mysql5_debian_sarge
# http://mapopa.blogspot.com/2009/03/compiling-mysql6.html

# get mysql source from site

sudo apt-get install build-essential
sudo apt-get install libncurses5 libncurses5-dbg libncurses-dev
# for some reason I am install lib32ncurses5 lib32ncurses5-dev - which is a screen dump...
sudo apt-get install autoconf libtool


# add options to base configuration in ./BUILD/Setup.sh
base_configs="$base_configs --with-innodb"
base_configs="$base_configs --with-federated-storage-engine"
base_configs="$base_configs --with-partition"

# Auto Build it quick and easy
sh ./BUILD/compile-amd64-max --prefix=/srv/mysql5_4 
sudo make
sudo make install

sudo cp mysql.server /etc/init.d/
sudo mkdir /etc/mysql
sudo cp my-large.cnf /etc/mysql/my.cnf
sudo ln -s /etc/mysql/my.cnf /etc/my.cnf

sudo mkdir /mnt/ssd/mysql_data

sudo pico /etc/mysql/my.cnf
 [mysqld]
 basedir=/srv/mysql5_4
 datadir=/mnt/ssd/mysql_data
 bind-address = 0.0.0.0
 max_connections=500

sudo pico /etc/init.d/mysql.server
 basedir=/srv/mysql5_4
 datadir=/mnt/ssd/mysql_data

sudo groupadd mysql
sudo useradd -g mysql mysql

cd /srv/mysql5_4
sudo chown -R mysql .
sudo chgrp -R mysql .

# install the defaults (data)
sudo ./bin/mysql_install_db --user=mysql --datadir=/mnt/ssd/mysql_data

sudo ln -s /srv/mysql5_4/bin/mysql /usr/local/bin/


# manual configure options - configure it on your own
#./configure --help
# run configure - i don't set the data directory here, I set it in the my.cnf
# prev: ./configure --prefix=/srv/mysql6_11
#./configure --prefix=/srv/mysql5_4 --with-plugins=innodb
#make
#sudo make install


# work around - edit ./my.cnf and comment -> #skip-federated 

# setup the default system
#./bin/mysql_install_db --user=mysql --basedir=/srv/mysql6_11 --builddir=/home/branflake2267/downloads/mysql-6.0.11-alpha --datadir=/srv/mysql_data

sudo ./bin/mysql_install_db --user=mysql --basedir=/srv/mysql5_4 --builddir=/home/branflake2267/downloads/mysql-5.4.0-beta --datadir=/srv/mysql_data


# start server
sudo /etc/init.d/mysql.server start

# mysql - no password exists for root localhost user yet
./bin/mysql -uroot -p  

# remove user root:%

# add user
CREATE USER 'Branflake2267'@'localhost' IDENTIFIED BY 'password*7';
GRANT ALL PRIVILEGES ON *.* TO 'Branflake2267'@'localhost' WITH GRANT OPTION;
flush privileges;


# edit /etc/hosts and add hostnames that connect to your server







#mysqls help./b
shell> groupadd mysql
shell> useradd -g mysql mysql
shell> gunzip < mysql-VERSION.tar.gz | tar -xvf -
shell> cd mysql-VERSION
shell> ./configure --prefix=/usr/local/mysql
shell> make
shell> make install
shell> cp support-files/my-medium.cnf /etc/my.cnf
shell> cd /usr/local/mysql
shell> chown -R mysql .
shell> chgrp -R mysql .
shell> bin/mysql_install_db --user=mysql
shell> chown -R root .
shell> chown -R mysql var
shell> bin/mysqld_safe --user=mysql &
```

# Other Links #
  * http://ubuntuforums.org/showthread.php?t=93725