from setuptools import setup, find_packages

setup(
    name='airflow-custom-operator',
    version='0.1.0',
    author='Your Name',
    author_email='your.email@example.com',
    description='A custom Apache Airflow operator',
    packages=find_packages(),
    install_requires=[
        'apache-airflow>=2.0.0',
    ],
    classifiers=[
        'Programming Language :: Python :: 3',
        'License :: OSI Approved :: Apache Software License',
        'Operating System :: OS Independent',
    ],
    python_requires='>=3.6',
)