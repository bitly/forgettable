from setuptools import setup, find_packages


setup(
    name="forgettable",
    version="0.1.0",
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
    url="https://github.com/bitly/forgettable/tree/master/pyforget",
    maintainer="Saul Shanabrook",
    maintainer_email="s.shanabrook@gmail.com",
)
