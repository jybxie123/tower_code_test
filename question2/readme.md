# Preparation

Go to the question2/src/config.py to modify the root path to your question2 folder's path.

I recommend using the conda environment: (If it exists, skip this)
```shell
conda create -n tower_code_test python=3.11 -y
conda activate tower_code_test
cd question2
pip install -r requirements.txt
```

# Quick start
```shell
cd question2/src
python main.py
```

# Recompile c++ functions:

1. please install pybind11 and setuptools first.
```shell
# cd /path/to/your/repository
cd question2/src
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


