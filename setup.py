#import ez_setup
#ez_setup.use_setuptools()
from setuptools import setup, find_packages
setup(name='ecopadq',
      version='0.0',
      packages= find_packages(),
      package_data={'ecopadq':['ecopadq/tasks/templates/*.tmpl']},
      install_requires=[
          'celery',
          'requests',
          'jinja2',
      ],
     dependency_links=[
          'https://github.com/ouinformatics/dockertask/archive/0.0.zip',
      ],
      include_package_data=True,
)
