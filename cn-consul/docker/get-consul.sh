VERSION=0.6.0
wget https://releases.hashicorp.com/consul/${VERSION}/consul_${VERSION}_web_ui.zip
wget https://releases.hashicorp.com/consul/${VERSION}/consul_${VERSION}_linux_amd64.zip
unzip consul_${VERSION}_linux_amd64.zip
mv consul consul_linux
mkdir ui
cd ui
unzip ../*_web_ui.zip
cd ..


