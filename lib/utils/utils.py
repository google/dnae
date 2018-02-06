# Copyright 2018 Google Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Utility modules - Misc utility methods."""

import calendar
import datetime
import logging
import re
import time


class TextUtils(object):
  """Provides text/string related utility methods."""

  def __init__(self):
    pass

  @classmethod
  def toascii(cls, input_string):
    temp_string = input_string
    return str(re.sub(r"[^\x00-\x7F]+", "?", temp_string))

  @classmethod
  def removecommas(cls, input_string):
    return cls.__replace(input_string, ",", " ")

  @classmethod
  def removequotes(cls, input_string):
    return cls.__replace(input_string, '"', "")

  @classmethod
  def removenewlines(cls, input_string):
    temp_string = cls.__replace(input_string, "\r", "")
    temp_string = cls.__replace(temp_string, "\n", "")
    return temp_string

  @classmethod
  def timestamp(cls, mode=None):
    if mode == "short":
      out = time.strftime("%Y%m%d")
    else:
      out = time.strftime("%Y%m%d_%H%M%S")
    return out

  @classmethod
  def toidentifier(cls, input_string):
    out = cls.toascii(input_string)
    out = (out.replace(" ", "_").replace("&", "n").replace("@", "_")
           .replace("#", "hash").replace("%", "x100").replace("'", "_")
           .replace("+", "plus").replace("-", "minus").replace("*", "x")
           .replace("/", "div").replace(".", "point").replace(",", "comma")
           .replace("(", "_").replace(")", "_").replace("[", "_")
           .replace("]", "_").replace("{", "_").replace("}", "_"))
    return out

  @classmethod
  def strlist2str(cls, string_list, sep):
    out = ""
    for item in string_list:
      out += item
      out += sep
    return out[:-1]

  @classmethod
  def __replace(cls, input_string, old, new):
    replacefcn = lambda s: s.replace(old, new)
    if isinstance(input_string, str):
      out = replacefcn(input_string)
    elif isinstance(input_string, list):
      out = list()
      for item in input_string:
        if isinstance(item, str):
          out.append(replacefcn(item))
        else:
          out.append(item)
    else:
      out = None
    return out


class DateUtils(object):
  """Provides Date related utility methods."""

  today = datetime.datetime.today()
  curryear = today.year
  one_day = datetime.timedelta(days=1)

  quartermap = dict()

  quartermap[1] = [
      datetime.datetime(curryear, 1, 1),
      datetime.datetime(curryear, 3, 31, 23, 59, 59, 999999)
  ]
  quartermap[2] = [
      datetime.datetime(curryear, 4, 1),
      datetime.datetime(curryear, 6, 30, 23, 59, 59, 999999)
  ]
  quartermap[3] = [
      datetime.datetime(curryear, 7, 1),
      datetime.datetime(curryear, 9, 30, 23, 59, 59, 999999)
  ]
  quartermap[4] = [
      datetime.datetime(curryear, 10, 1),
      datetime.datetime(curryear, 12, 31, 23, 59, 59, 999999)
  ]

  def __init__(self):
    pass

  @classmethod
  def usec2date(cls, usec):
    d0 = datetime.datetime(year=1970, month=1, day=1)  # unix time reference
    delta = datetime.timedelta(microseconds=usec)
    return d0 + delta

  @classmethod
  def quarterstartdate(cls):
    curr_quarter = (DateUtils.today.month - 1) / 3 + 1
    quarter_start_date = cls.quartermap[curr_quarter][0]
    return quarter_start_date

  @classmethod
  def quarterenddate(cls):
    curr_quarter = (DateUtils.today.month - 1) / 3 + 1
    quarter_end_date = cls.quartermap[curr_quarter][1]
    return quarter_end_date

  @classmethod
  def lastdayofmonth(cls, month):
    return calendar.monthrange(cls.curryear, month)[1]

  @classmethod
  def dbmdate2sfdate(cls, datestr):
    return datetime.datetime.strptime(datestr, "%Y/%m/%d").strftime("%Y-%m-%d")

  @classmethod
  def firstdayofquarter(cls):
    t = datetime.datetime.today().toordinal()
    if t in [
        datetime.datetime(cls.curryear, 1, 1).toordinal(),
        datetime.datetime(cls.curryear, 4, 1).toordinal(),
        datetime.datetime(cls.curryear, 7, 1).toordinal(),
        datetime.datetime(cls.curryear, 10, 1).toordinal()
    ]:
      return True
    else:
      return False

  @classmethod
  def firstdayofyear(cls):
    t = datetime.datetime.today().toordinal()
    if t == datetime.datetime(cls.curryear, 1, 1).toordinal():
      return True
    else:
      return False

  @classmethod
  def quarterdays(cls):
    return cls.quarterenddate().toordinal() - cls.quarterstartdate().toordinal(
    ) + 1

  @classmethod
  def dayofquarter(cls, date=None, date_format=None):
    if not date:
      date = cls.today
    else:
      date = datetime.datetime.strptime(date, date_format)

    q2 = (datetime.datetime.strptime("4/1/{0:4d}".format(date.year),
                                     "%m/%d/%Y")).timetuple().tm_yday
    q3 = (datetime.datetime.strptime("7/1/{0:4d}".format(date.year),
                                     "%m/%d/%Y")).timetuple().tm_yday
    q4 = (datetime.datetime.strptime("10/1/{0:4d}".format(date.year),
                                     "%m/%d/%Y")).timetuple().tm_yday

    cur_day = date.timetuple().tm_yday

    if date.month < 4:
      return cur_day
    elif date.month < 7:
      return cur_day - q2 + 1
    elif date.month < 10:
      return cur_day - q3 + 1
    else:
      return cur_day - q4 + 1


class SelfIncreasingIndex(object):
  """Provides utility methods to create and use a self-increasing index."""

  def __init__(self):
    self.__value = 0

  def __call__(self, *args, **kwargs):
    val = self.__value
    self.__value += 1
    return val

  def start(self, init_value=0):
    self.__value = init_value
    return self.__value

  def nextval(self):
    self.__value += 1
    return self.__value


# Decorators
def retry(some_function, *args, **kwargs):

  _MAX_RETRY = 5

  def wrapper(*args, **kwargs):
    retval = None
    retry_attempts = 0
    done = False
    while not done:
      try:
        retval = some_function(*args, **kwargs)
        done = True
      # pylint: disable=broad-except
      except Exception as error:
        retry_attempts += 1
        if retry_attempts <= _MAX_RETRY:
          seconds = 2 ** retry_attempts
          logging.warning("Encountered an error - %s -, "
                          "retrying in %d seconds...", str(error), seconds)
          time.sleep(seconds)
        else:
          raise error
      # pylint: enable=broad-except
    return retval

  return wrapper
