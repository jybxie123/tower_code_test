from setuptools import setup, Extension
import pybind11

ext_modules = [
    Extension(
        'cal_beta',
        ['cal_beta.cpp'],
        include_dirs=[
            pybind11.get_include(),
            '/opt/homebrew/anaconda3/envs/tower_code_test/include/python3.11',
            '/opt/homebrew/anaconda3/envs/tower_code_test/lib/python3.11/site-packages/pybind11/include',
        ],
        language='c++',
        extra_compile_args=['-O3', '-march=native', '-std=c++17'] 
    ),
]

setup(
    name='cal_beta',
    version='0.0.1',
    author='Your Name',
    author_email='your.email@example.com',
    description='A module to calculate beta values using pybind11',
    ext_modules=ext_modules,
    install_requires=['pybind11'],
    zip_safe=False,
)
