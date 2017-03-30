from celery.task import task
from celery import Celery
from celery import chain
from celery import signature
from time import sleep
from random import randint

import logging

logging.basicConfig(level=logging.INFO)

@task()
def random_delay_in(*args, **kwargs):
    seconds =  randint(10,60)
    logging.info(args)
    logging.info(kwargs)
    logging.info("sleeping for: %s" % seconds)
    sleep(seconds)
    return True

@task()
def launch():
    logging.info("launching...")

    first = signature("repotestq.tasks.tasks.random_delay_in")
    second = signature("repotestq.tasks.tasks.random_delay_in")
    third = signature("repotestq.tasks.tasks.random_delay_in") 
    fourth = signature("repotestq.tasks.tasks.random_delay_in")

    res = first.apply_async(args=("first"), link=chain(second("second"), third("third"), fourth("fourth")))
    
    logging.info("finished...")

    return res
