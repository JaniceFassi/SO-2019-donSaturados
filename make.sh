cd ..
git clone https://github.com/sisoputnfrba/so-commons-library
cd so-commons-library
sudo make install
cd ..
#git clone https://github.com/sisoputnfrba/fifa-examples
cd tp-2019-1c-donSaturados
cd sharedLibrary/Debug
make clean
make all
sudo mv libsharedLibrary.so /usr/lib
cd ../..
cd LFS/lissandra/Debug
make clean
make all
cd ../../..
cd memoryPool/Debug
make clean
make all
cd ../..
cd kernel/Debug
make clean
make all
cd ../..
git clone https://github.com/sisoputnfrba/1C2019-Scripts-lql-entrega.git
cd ..
