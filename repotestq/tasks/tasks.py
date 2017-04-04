from celery.task import task
from celery import Celery
from celery import chain
from celery import signature
from time import sleep
from random import randint

import logging

logging.basicConfig(level=logging.INFO)

@task(bind=True)
def random_delay_in(self,*args, **kwargs):
    seconds =  randint(10,60)
    logging.info(args)
    logging.info(kwargs)
    logging.info("sleeping for: %s" % seconds)
    sleep(seconds)
    return {"status": "Sub task Complete"}


@task()
def launch():
    logging.info("launching...")

    first = signature("repotestq.tasks.tasks.random_delay_in", kwargs={'first': True})
    second = signature("repotestq.tasks.tasks.random_delay_in", args=("987654321"))
    third = signature("repotestq.tasks.tasks.random_delay_in", kwargs={'mmsid': "987654321"}) 
    fourth = signature("repotestq.tasks.tasks.random_delay_in")

    workflow = (first | second | third | fourth)
    
    res = workflow("testing")

    logging.info(res)
   
    logging.info("finished...")

    return {"status": "SUCCESS"}


