from setuptools import setup, find_packages

setup(name='schemaman',
      version='0.1.12',
      description='SchemaMan is a schema manager and query wrapper for multiple database backends, with Version and Change Management.',
      url='http://github.com/ghowland/schemaman',
      author='Geoff Howland',
      author_email='geoff@gmail.com',
      license='MIT',
      packages=['schemaman'],
      include_package_data=True,
      data_files=[('', ['LICENSE', 'README.md'])],
      classifiers=[
        'Development Status :: 3 - Alpha',
        'License :: OSI Approved :: MIT License',
        'Programming Language :: Python :: 2.7',
        'Topic :: Database :: Front-Ends',
      ],
      entry_points = {
        'console_scripts': ['schemaman=schemaman.schemaman:Main'],
      },
      zip_safe=False)

