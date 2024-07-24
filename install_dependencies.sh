wget -O - https://apt.llvm.org/llvm-snapshot.gpg.key | sudo apt-key add -
sudo add-apt-repository 'deb http://apt.llvm.org/focal/ llvm-toolchain-focal-13 main'
sudo apt update
sudo apt install -y build-essential libtbb-dev librocksdb-dev libhiredis-dev libaio-dev libconfig-dev libxxhash-dev libssl-dev libnuma-dev libjemalloc-dev clang-13 clang-format-13 python3 python3-pip
python3 -mpip install pandas