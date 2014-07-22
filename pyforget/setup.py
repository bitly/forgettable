from setuptools import setup, find_packages


setup(
    name="forgettable",
    version="0.0.0",
    packages=find_packages(),
    entry_points={
        'console_scripts': [
            'forgettable=forgettable.server:main'
        ]
    },
    install_requires=[
        'tornado',
        'numpy',
        'redis',
    ],

    long_description=open('readme.md').read(),
    author="bitly",
    url="https://github.com/bitly/forgettable/tree/master/pyforget",
)
