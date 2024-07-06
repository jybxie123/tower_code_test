# preparation
go to the config file to modify the root path to your towerpath

# quick start
```shell
git clone my repo:
git@github.com:jybxie123/tower_code_test.git
cd tower_code_test/question1
./quick_start.sh
```

# start manually
```shell
git clone my repo:
git@github.com:jybxie123/tower_code_test.git
cd tower_code_test/question1/src
python matrix.py
```
go to another shell, go to the same path:
```shell
cd tower_code_test/question1/src
python correlation.py
```
you can check the log file to see the details.



# Recompile your c++ functions:

1. please install pybind11 first.
```shell
# cd /path/to/your/repository
cd src
pip install pybind11
```
2. use these commands to find the correct package path for your setup.py
```shell
python -c "import pybind11; print(pybind11.get_include())"
python3-config --includes
```
3. Modify other configuration items to match your OS. For the auther, it is MACOS.
4. run this command to compile your setup.py
```shell
python pearson/setup.py build_ext --inplace
```
then you can find the .so file in the path




