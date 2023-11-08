# -*- coding: utf-8 -*-
"""
Created on Wed Sep 13 22:41:35 2023

@author: abbes
"""

import time

from collect import collecting

def job():
    print("Fetching YouTube data...")
    collecting()  # Call your YouTube data fetching function here

# Schedule the job to run every hour
i=10
while i>0:
    job()
    time.sleep(3600)
    i=i-1
