from setuptools import setup, find_packages

setup(
    name="rv-sqltools",
    version='1.0',
    packages=find_packages(exclude=['tests']),
    install_requires=[

    ],
    python_requires='>=3',
)