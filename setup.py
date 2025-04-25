from setuptools import setup, find_packages

setup(
    name='livy-session-operator',
    version='0.1.0',
    author='John Metzger',
    author_email='john.metzger@microsoft.com',
    description='A custom Apache Airflow operator to interact with Apache Livy sessions in Microsoft Fabric.',
    packages=find_packages(),
    install_requires=[
        'apache-airflow>=2.0.0',
        'apache-airflow-providers-apache-livy>=4.0.0',
        'apache_airflow_microsoft_fabric_plugin>=0.1.0'
    ],
    classifiers=[
        'Programming Language :: Python :: 3',
        'License :: OSI Approved :: Apache Software License',
        'Operating System :: OS Independent',
    ],
    python_requires='>=3.6',
)