# Preparation
Go to the question1/src/config.py to modify the root path to your question1 folder's path.

I recommend using the conda environment:
```shell
conda create -n tower_code_test python=3.11 -y
conda activate tower_code_test
cd question1
pip install -r requirements.txt
```

# Quick start
```shell
cd question1
./quick_start.sh
```

# Start manually
Note that you must run the matrix.py before correlation.py. 
Otherwise, the program will fail. I think this is in line with expectations.
```shell
cd question1/src
python matrix.py
```
go to another shell, go to the same path:
```shell
cd question1/src
python correlation.py
```
For question1, you can check the log file to see the details.
I will print the correlation matrix in the stdout, to check the correctness.

# Recompile c++ functions:

1. please install pybind11 and setuptools first.
```shell
cd question1/src
pip install setuptools pybind11
```
1. use these commands to find the correct path in ext_modules for your setup.py
```shell
python -c "import pybind11; print(pybind11.get_include())"
python3-config --includes
```
2. Modify other configuration items in setup.py to match your OS. For the auther, it is MACOS.
3. Run this command to compile your setup.py
```shell
python pearson/setup.py build_ext --inplace
```
then you can find the .so file in the path




