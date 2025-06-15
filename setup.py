from setuptools import setup, find_packages

long_description=open('README.md').read()
long_description_content_type='text/markdown'

setup(
    name='tw_crawler',
    version='v1.2.4',
    description='A crawler for Taiwan stock market data',
    long_description=long_description,
    long_description_content_type=long_description_content_type,
    packages=find_packages(),
    install_requires=[
        'requests',
        'cloudscraper',
        'pandas',
    ],
    author='nk7260ynpa',
    author_email='nk7260ynpa@gmail.com',
    url='https://github.com/nk7260ynpa/Tw_stock_crawler',
    classifiers=[
        'Programming Language :: Python :: 3',
        'Development Status :: 3 - Alpha',
        'Intended Audience :: Developers',
        'Topic :: Software Development :: Libraries :: Python Modules',
        'License :: OSI Approved :: MIT License',
        'Operating System :: OS Independent',
    ],
    python_requires='>=3.8',
)
