from setuptools import setup

setup(
    name='erc_cli',
    version='1.0',
    scripts=["main.py"],
    entry_points={
        'console_scripts': [
            'erc-cli=main:main',
        ]
    }
)