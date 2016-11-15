# start ec2 instance in us-east-1 (ssh key ec2-east.pem) with volume snap-753dfc1c as 2nd ebs volume with at least 320GB
mkdir /tmp/data
sudo mount /dev/xvdb /tmp/data
cd /tmp/data/wikistats/pagecounts
sudo tar -cvzf /tmp/data/subset.tgz pagecounts-2008110*gz 
pagecounts-20081101-000001.gz
