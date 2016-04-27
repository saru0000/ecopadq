from celery.task import task
from dockertask import docker_task
from subprocess import call,STDOUT
from jinja2 import Template
from shutil import copyfile
import requests,os

#Default base directory 
basedir="/data/static/"
host= 'ecolab.cybercommons.org'
host_data_dir = "/home/ecopad/ecopad/data/static"


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
def teco_spruce_simulation(pars): # ,model_type="0", da_params=None):
    """ Setup task convert parameters from html portal
	to file, and store the file in input folder.
	call teco_spruce_model.
    """
    task_id = str(teco_spruce_simulation.request.id)
    resultDir = setup_result_directory(task_id)
    
    #tmpl = os.path.join(os.path.dirname(__file__),'templates/spruce_pars.tmpl')
    #with open(tmpl,'r') as f:
    #    template=Template(f.read())
    #params_file = os.path.join(resultDir,'spruce_pars.txt')
    #with open(params_file,'w') as f2:
    #    f2.write(template.render(check_params(pars))) 
    param_filename = create_template('spruce_pars',pars,resultDir,check_params)
    #Run Spruce TECO code 
    host_data_resultDir = "{0}/ecopad_tasks/{1}".format(host_data_dir,task_id)
    docker_opts = "-v %s:/data:z " % (host_data_resultDir)
    docker_cmd = "{0} {1} {2} {3} {4} {5}".format("/data/spruce_pars.txt","/source/input/SPRUCE_forcing.txt",
                                    "/source/input/SPRUCE_obs.txt",
                                    "/data", 0 , "/source/input/SPRUCE_da_pars.txt")
    print (docker_cmd)
    result = docker_task(docker_name="teco_spruce",docker_opts=docker_opts,docker_command=docker_cmd,id=task_id)
    #Run R Plots
    docker_opts = "-v %s:/usr/local/src/myscripts/graphoutput:z " % (host_data_resultDir)
    docker_cmd = None
    result = docker_task(docker_name="ecopad_r",docker_opts=docker_opts,docker_command=docker_cmd,id=task_id)
    return "http://%s/ecopad_tasks/%s" % (result['host'],result['task_id']) 
  
@task()
def teco_spruce_data_assimilation(pars,da_params=None):
    """
        DA TECO Spruce
    """
    task_id = str(teco_spruce_data_assimilation.request.id)
    resultDir = setup_result_directory(task_id)
    #parm template file
    param_filename = create_template('spruce_pars',pars,resultDir,check_params)
    if da_params:
        da_param_filename = create_template('spruce_da_pars',da_params,resultDir,check_params)
    else:
        copyfile("{0}/ecopad_tasks/default/SPRUCE_da_pars.txt".format(basedir),resultDir)
        da_param_filename ="SPRUCE_da_pars.txt"
    #Run Spruce TECO code
    host_data_resultDir = "{0}/ecopad_tasks/{1}".format(host_data_dir,task_id)
    docker_opts = "-v %s:/data:z " % (host_data_resultDir)
    docker_cmd = "{0} {1} {2} {3} {4} {5}".format("/data/{0}".format(param_filename),"/source/input/SPRUCE_forcing.txt",
                                    "/source/input/SPRUCE_obs.txt",
                                    "/data",1, "/data/{0}".format(da_param_filename))
    result = docker_task(docker_name="teco_spruce",docker_opts=docker_opts,docker_command=docker_cmd,id=task_id)
    #Run R Plots

@task()
def teco_spruce_forecast(pars,forecast_year,forecast_day,da_params=None,temperature_treatment=0.0,co2_treatment=380.0,da_task_id=None):
    """
        Forecasting 
    """
    task_id = str(teco_spruce_data_assimilation.request.id)
    resultDir = setup_result_directory(task_id)
    param_filename = create_template('spruce_pars',pars,resultDir,check_params)
    #da_param_filename = create_template('spruce_da_pars',da_params,resultDir,check_params)
    if not da_task_id:
        da_task_id = "default"
    copyfile("{0}/ecopad_tasks/{1}/Paraest.txt".format(basedir,da_task_id),resultDir)
    if da_params:
        da_param_filename = create_template('spruce_da_pars',da_params,resultDir,check_params)
    else:
        copyfile("{0}/ecopad_tasks/default/SPRUCE_da_pars.txt".format(basedir),resultDir)
        da_param_filename ="SPRUCE_da_pars.txt"
    #Run Spruce TECO code
    host_data_resultDir = "{0}/ecopad_tasks/{1}".format(host_data_dir,task_id)
    docker_opts = "-v %s:/data:z " % (host_data_resultDir)
    docker_cmd = "{0} {1} {2} {3} {4} {5} {6} {7} {8} {9} {10}".format("/data/{0}".format(param_filename),
                                    "/source/input/SPRUCE_forcing.txt", "/source/input/SPRUCE_obs.txt",
                                    "/data",2, "/data/{0}".format(da_param_filename),
                                    "/source/input/Weathergenerate",forecast_year, forecast_day,
                                    temperature_treatment,co2_treatment)
    result = docker_task(docker_name="teco_spruce",docker_opts=docker_opts,docker_command=docker_cmd,id=task_id)
    #Run R Plots


def create_template(tmpl_name,params,resultDir,check_function):
    tmpl = os.path.join(os.path.dirname(__file__),'templates/{0}.tmpl'.format(tmpl_name))
    with open(tmpl,'r') as f:
        template=Template(f.read())
    params_file = os.path.join(resultDir,'{0}.txt'.format(tmpl_name))
    with open(params_file,'w') as f2:
        f2.write(template.render(check_function(params)))
    return '{0}.txt'.format(tmpl_name)

def setup_result_directory(task_id):
    resultDir = os.path.join(basedir, 'ecopad_tasks/', task_id)
    os.makedirs(resultDir)
    return resultDir 

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
