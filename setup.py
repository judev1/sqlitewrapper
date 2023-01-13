from setuptools import setup, find_packages

setup(
    name='sqlitewrapper',
    version='0.1.3',
    license='MIT',
    author='Jude',
    author_email='jude.cowden@protonmail.com',
    packages=find_packages(),
    url='https://github.com/judev1/sqlitewrapper',
    description='A python object-oriented wrapper for sqlite',
    long_description=open('README.md').read(),
    long_description_content_type='text/markdown',
    classifiers=[
        'Development Status :: 5 - Production/Stable',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: MIT License',
        'Programming Language :: Python :: 3'
    ]
)