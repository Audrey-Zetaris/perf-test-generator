#!/bin/bash

#######################################
# Simple EC2 Ubuntu Setup
# For git clone approach
#######################################

echo "Setting up EC2 Ubuntu for Delta Lake BMT..."

# Update system
sudo apt update && sudo apt upgrade -y

# Install essential tools
sudo apt install -y curl wget git unzip htop vim awscli

# Install Java 17 (required for Spark 3.5.1)
sudo apt install -y openjdk-17-jdk
echo 'export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64' >> ~/.bashrc

# Install SBT
echo "deb https://repo.scala-sbt.org/scalasbt/debian all main" | sudo tee /etc/apt/sources.list.d/sbt.list
curl -sL "https://keyserver.ubuntu.com/pks/lookup?op=get&search=0x2EE0EA64E40A89B84B2DF73499E82A75642AC823" | sudo apt-key add
sudo apt update && sudo apt install -y sbt

# Install Spark 3.5.1
cd /tmp
wget https://archive.apache.org/dist/spark/spark-3.5.1/spark-3.5.1-bin-hadoop3.tgz
tar -xzf spark-3.5.1-bin-hadoop3.tgz
sudo mv spark-3.5.1-bin-hadoop3 /opt/spark
echo 'export SPARK_HOME=/opt/spark' >> ~/.bashrc
echo 'export PATH=$PATH:$SPARK_HOME/bin' >> ~/.bashrc

# Apply environment
source ~/.bashrc

echo "✅ Setup complete!"
echo ""
echo "Next steps:"
echo "1. Clone repo: git clone <your-repo>"
echo "2. cd perf-test-generator"
echo "3. sbt assembly"
echo "4. cp target/scala-2.12/*.jar lib/"
echo "5. Update conf/generator.conf for production"
echo "6. ./bin/start-generator.sh"