from setuptools import setup, Extension
from setuptools.command.build_ext import build_ext
import pybind11

class get_pybind_include(object):
    """Helper class to determine the pybind11 include path
    The purpose of this class is to delay importing pybind11 until it is actually installed,
    so that the `get_include()` method can be invoked. """
    def __str__(self):
        return pybind11.get_include()

ext_modules = [
    Extension(
        'pearson',
        ['pearson_correlation.cpp'],
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
    name='pearson',
    version='0.0.1',
    author='jybxie',
    author_email='jiangyanbo8800@163.com',
    description='Pearson correlation in C++ with Python binding',
    ext_modules=ext_modules,
    install_requires=['pybind11'],
    cmdclass={'build_ext': build_ext},
    zip_safe=False,
)
