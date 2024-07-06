# preparation
```shell
git clone git@github.com:jybxie123/tower_code_test.git
cd tower_code_test/question1/src
```
go to the config.py to modify the root path to your towerpath


# quick start
```shell
cd tower_code_test/question2/src
python main.py
```

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


