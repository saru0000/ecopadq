from celery.task import task
from dockertask import docker_task
from subprocess import call,STDOUT
from jinja2 import Template
import requests

#Default base directory 
basedir="/data/static/"
host= 'ecolab.cybercommons.org'

#Example task
@task()
def add(x, y):
    """ Example task that adds two numbers or strings
        args: x and y
        return addition or concatination of strings
    """
    result = x + y
    return result
@task()
def teco_spruce_setup(pars):
    """ Setup task convert parameters from html portal
	to file, and store the file in input folder.
	Then call teco_spruce_run()
    """
    task_id = str(teco_spruce_setup.request.id)
    resultDir = os.path.join(basedir, 'ecopad_tasks/', task_id)
    os.makedirs(resultDir)
    with open('templates/spruce_pars.tmpl','r') as f:
        template=Template(f.read())
        with open(os.path.join(resultDir,'/spruce_pars.txt'),'w') as f2:
            f2.write(template.render(**pars)) 
    return "http://%s/mgmic_tasks/%s" % (host,task_id)   

def teco_spruce_run(pars,forcing,obs,output,MCMC):
    """ Run task compile teco_spruce fortran code
	args: pars is parameters from portal
	      forcing is climate forcing input
	      obs is observation data for DA
	      output is model output folder
	      MCMC is flag for DA(1) or simulation(0)
    """
