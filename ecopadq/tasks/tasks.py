from celery.task import task
from dockertask import docker_task
from subprocess import call,STDOUT
from jinja2 import Template
import requests,os

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
    
    tmpl = os.path.join(os.path.dirname(__file__),'templates/spruce_pars.tmpl')
    with open(tmpl,'r') as f:
        template=Template(f.read())
    params_file = os.path.join(resultDir,'spruce_pars.txt')
    with open(params_file,'w') as f2:
        f2.write(template.render(pars)) 
    #Run Spruce TECO code 
    host_data_resultDir = "/home/ecopad/ecopad/data/static/ecopad_tasks/%s" % (task_id)
    docker_opts = "-v %s:/data " % (host_data_resultDir)
    docker_cmd = "%s %s %s %s %s" % ("/data/spruce_pars.txt","/source/input/SPRUCE_forcing.txt",
                                    "/source/input/SPRUCE_forcing.txt",
                                    "/data","0")
    result = docker_task(docker_name="teco_spruce",docker_opts=docker_opts,docker_command=docker_cmd,id=task_id)
    return "http://%s/ecopad_tasks/%s" % (result['host'],result['task_id'])   

def teco_spruce_run(pars,forcing,obs,output,MCMC):
    """ Run task compile teco_spruce fortran code
	args: pars is parameters from portal
	      forcing is climate forcing input
	      obs is observation data for DA
	      output is model output folder
	      MCMC is flag for DA(1) or simulation(0)
    """
