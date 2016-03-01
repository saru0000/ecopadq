from celery.task import task
from dockertask import docker_task
from subprocess import call,STDOUT
import requests

#Default base directory 
#basedir="/data/static/"


#Example task
@task()
def add(x, y):
    """ Example task that adds two numbers or strings
        args: x and y
        return addition or concatination of strings
    """
    result = x + y
    return result

def teco_spruce_setup(pars):
    """ Setup task convert parameters from html portal
	to file, and store the file in input folder.
	Then call teco_spruce_run()
    """

def teco_spruce_run(pars,forcing,obs,output,MCMC):
    """ Run task compile teco_spruce fortran code
	args: pars is parameters from portal
	      forcing is climate forcing input
	      obs is observation data for DA
	      output is model output folder
	      MCMC is flag for DA(1) or simulation(0)
    """
