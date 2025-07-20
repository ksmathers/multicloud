#!/usr/bin/env python

from distutils.core import setup

setup(name='multicloud',
      version='0.01',
      description='Abstract cloud services API for multiple local and cloud based providers',
      author='Kevin Smathers',
      author_email='kevin@ank.com',
      url='https://github.com/ksmathers/multicloud',
      packages=['multicloud', 'multicloud.aws'],
      package_dir={
            'multicloud': 'multicloud',
            'multicloud.aws': 'multicloud_aws'
      },
      install_requires=[]
     )
