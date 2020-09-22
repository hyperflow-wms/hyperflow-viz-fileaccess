from setuptools import setup
from setuptools import find_packages

setup(
    name='hyperflow_viz_fileaccess',
    version='1.0',
    packages=find_packages(),
    url='https://github.com/hyperflow-wms/hyperflow-viz-fileaccess',
    license='',
    author='Kamil Noster',
    author_email='kamil.noster@gmail.com',
    description='Visualization tool for file block access for FBAM monitoring tool & HyperFlow WMS',
    install_requires=[
        'jsonlines~=1.2.0',
        'pandas~=1.1.2',
        'numpy~=1.19.2',
        'matplotlib~=3.3.2',
        'setuptools~=50.3.0'
    ],
    python_requires='>=3',
    entry_points={
        'console_scripts': [
            'hyperflow-viz-fileaccess=hyperflow_viz_fileaccess.main:main'
        ],
    }
)
