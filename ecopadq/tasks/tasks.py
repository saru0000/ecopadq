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
def teco_spruce_model(pars,model_type="0"):
    """ Setup task convert parameters from html portal
	to file, and store the file in input folder.
	call teco_spruce_model.
    """
    task_id = str(teco_spruce_model.request.id)
    resultDir = os.path.join(basedir, 'ecopad_tasks/', task_id)
    os.makedirs(resultDir)
    
    tmpl = os.path.join(os.path.dirname(__file__),'templates/spruce_pars.tmpl')
    with open(tmpl,'r') as f:
        template=Template(f.read())
    params_file = os.path.join(resultDir,'spruce_pars.txt')
    with open(params_file,'w') as f2:
        f2.write(template.render(check_params(pars))) 
    #Run Spruce TECO code 
    host_data_resultDir = "/home/ecopad/ecopad/data/static/ecopad_tasks/%s" % (task_id)
    docker_opts = "-v %s:/data:z " % (host_data_resultDir)
    docker_cmd = "/source/teco_spruce %s %s %s %s %s" % ("/data/spruce_pars.txt","/source/input/SPRUCE_forcing.txt",
                                    "/source/input/SPRUCE_obs.txt",
                                    "/data",str(model_type))
    result = docker_task(docker_name="teco_spruce",docker_opts=docker_opts,docker_command=docker_cmd,id=task_id)
    #Run R Plots
    docker_opts = "-v %s:/usr/local/src/myscripts/graphoutput:z " % (host_data_resultDir)
    docker_cmd = None
    result = docker_task(docker_name="ecopad_r",docker_opts=docker_opts,docker_command=docker_cmd,id=task_id)
    return "http://%s/ecopad_tasks/%s" % (result['host'],result['task_id'])   

def check_params(pars):
    """ Check params and make floats."""
    for param in ["latitude","longitude","wsmax","wsmin","LAIMAX","LAIMIN","SapS","SLA","GLmax","GRmax","Gsmax",
                    "extkU","alpha","Tau_Leaf","Tau_Wood","Tau_Root","Tau_F","Tau_C","Tau_Micro","Tau_SlowSOM",
                    "gddonset","Rl0" ]:
        if not "." in str(pars[param]):
            pars[param]="%s." % (str(pars[param]))
        else:
            pars[param]=str(pars[param]) 
    return pars    
