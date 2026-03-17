import math
import pandas as pd
from openpyxl import load_workbook
from datetime import datetime, timedelta
from ortools.sat.python import cp_model
from data_service import *

data = ShopfloorScheduler()

def data_preprocessing(dept_name: str,
                    sub_dept_name: str = "",)


def inline_rule_one(task_data):
    task_year = task_data[0]["Year"]
    task_month = task_data[0]["Month"]
    task_day = task_data[0]["Date"]


