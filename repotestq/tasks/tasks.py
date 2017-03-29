from celery.task import task
from celery import Celery
from celery import signature
from time import sleep
from random import randint

import logging

logging.basicConfig(level=logging.INFO)

@task()
def random_delay_in(msg):
    seconds =  randint(10,60)
    logging.info("sleeping for: %s" % seconds)
    sleep(seconds)
    return msg

@task()
def launch():
    logging.info("launching...")

    taskid = signature("repotestq.tasks.tasks.random_delay_in", ("first")).delay().id
    signature("repotestq.tasks.tasks.random_delay_in", ("second")).delay()
    signature("repotestq.tasks.tasks.random_delay_in", ("third")).delay() 
    signature("repotestq.tasks.tasks.random_delay_in", ("fourth")).delay()

    logging.info("finished...")
