wget -O - https://apt.llvm.org/llvm-snapshot.gpg.key | sudo apt-key add -
sudo add-apt-repository -y 'deb http://apt.llvm.org/focal/ llvm-toolchain-focal-13 main'
sudo apt update
sudo apt install -y build-essential libtbb-dev librocksdb-dev libhiredis-dev libaio-dev libconfig-dev libxxhash-dev libssl-dev libnuma-dev libjemalloc-dev clang-13 clang-format-13 python3 python3-pip
if [ ! -f /usr/bin/clang ]; then
    sudo ln -s /usr/bin/clang-13 /usr/bin/clang
fi
python3 -mpip install pandas

git clone https://github.com/vmware/splinterdb.git ../splinterdb