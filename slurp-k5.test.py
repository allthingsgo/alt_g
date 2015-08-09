#!/usr/bin/env python3

#######################################################################
# scrape k5 diaries, and download them, then extract diary/comment data into sw format
#
# Author: Garry Morrison
# email: garry.morrison -at- gmail.com
# Date: 2015-04-22
# Update: 2015-08-09 by sye 
# Copyright: GPL.v3 
#
# Usage: ./slurp-diaries.py hi lo section_name 
# download pages in reverse order range( early_page, late_page, -1)
#
#######################################################################

import sys
import os.path
import time
import urllib.request
#from urllib.request import urlopen
from bs4 import BeautifulSoup
import gzip
import re
#import json

from the_semantic_db_code import *
from the_semantic_db_functions import *
from the_semantic_db_processor import *

C = context_list("k5 by Section ")

if len(sys.argv) < 4:
  print("\nUsage:",sys.argv[0],"pages", "start_page", "Section_Name\n")
  sys.exit(1)
hi = int(sys.argv[1])
lo = int(sys.argv[2])
s_name = sys.argv[3]


# time between k5 page retrievals:
#sleep_time = 30
#sleep_time = 10
sleep_time = 1

# number of missed k5 pages before giving up:
miss_threshold = 5       

###

destination = "data/test-" + s_name
if not os.path.exists(destination):
  os.makedirs(destination)

tmp_destination = "data/tmp"
if not os.path.exists(tmp_destination):
  os.makedirs(tmp_destination)


# place to save data on k5ids:
#k5id_file = "sw-examples/k5id.sw"
k5_file = "sw-examples/test-" + s_name + ".sw"
if not os.path.exists("sw-examples/"):
  os.makedirs("sw-examples")


# file that keeps track of how many times we have hit k5:
download_tally_file="wget-download-test"
if not os.path.exists(download_tally_file):
  download_tally = 0
else:
  f = open(download_tally_file,mode='r')
  download_tally = int(f.read())
  f.close()


# given a url, download the html:
def download_html(url):
  global download_tally
  headers = { 'User-Agent' : 'Mozilla/5.0' }
  req = urllib.request.Request(url,None,headers)

# put in a retry for loop:
  for k in range(miss_threshold):
    try:
      f = urllib.request.urlopen(req)
      break
    except Exception as e:             # k5 regularly bugs out, so we need this.
      print(e.reason)
      print("Trying again ... ")
      sys.stdout.flush()
      time.sleep(30)                                # hopefully 30s is enough to solve the issue. Otherwise, may as well bail anyway.
      if (k + 1) >= miss_threshold:
        print("url request failed",miss_threshold,"times. Exiting!\n")
        sys.exit(0)

  if f.info().get('Content-Encoding') == 'gzip':  # sometimes WP spits back gzip, even if not requested.
    html = gzip.decompress(f.read())
    html = str(html)
  else:
    html = f.read()
  f.close()
  download_tally += 1
  return html

# download the given diary url:
def download_diary(url,k5id):
  diary_file = destination + "/" + k5id + ".html"
  print("diary file:",diary_file)
  diary = download_html(url + "?commentmode=nested&commenttype=all")
  f = open(diary_file,'wb')
  f.write(diary)
  f.close()
  return diary


# delete/escape chars we don't want inside kets:
def chomp_bad_chars(s):
# filter down to ascii only, escape \n, delete \r:
  s = s.encode('ascii','ignore').decode('ascii').replace('\n','\\n').replace('\r','')

# some escape:
  s = s.replace('&','&amp;').replace('<','&lt;').replace('>','&gt;')

# some more escapes:
  s = s.replace(':','&colon;').replace('|','&pipe;')

  return s


# process the given diary:
# should we have try/except wrapper in here? Probably!
def process_diary(context,diary):
  soup = BeautifulSoup(diary,"html.parser")

# first, extract the diary details:
  title = chomp_bad_chars(soup.title.text.replace(' || kuro5hin.org',''))
  element = soup('table',width='100%',border='0',cellpadding='3',cellspacing='0')
  line = element[0]
#  intro = line.find_all('tr')[1].find_all('td')[1].text.strip().encode('ascii', 'ignore').decode('ascii').replace('\n','\\n').replace('\r','')
  intro = chomp_bad_chars(line.find_all('tr')[1].find_all('td')[1].text.strip())
  datetime, tags = line.tr.find_all('td')[1].find_all('br')[2].text.replace(' (all tags)','').split(' Tags: ')
  kuron = line.tr.find_all('td')[1].find_all('br')[1].a.text
  relative_url = line.find_all('a')[1]['href']
  line = element[1]
#  body = line.find_all('font')[-1].text.strip().encode('ascii', 'ignore').decode('ascii').replace('\n','\\n').replace('\r','')
  body = chomp_bad_chars(line.find_all('font')[-1].text.strip())

# now some tidying up:
  diary_id = "diary: " + "-".join(relative_url[7:].split('/'))
  url = "http://www.kuro5hin.org" + relative_url
  date = "-".join(diary_id[7:].split('-')[:3])
  tags_sp = superposition()
  for tag in tags.split(', '):
    tags_sp += ket(chomp_bad_chars(tag))
  author = "kuron: " + kuron
  intro_wc = len(intro.split())  # NB: word counts are not strings. We need to cast them to str() when we learn them.
  body_wc = len(body.split())
  total_wc = intro_wc + body_wc

# now learn it all:
  context.learn("url",diary_id,"url: " + url)
  context.learn("date",diary_id,"date: " + date)
  context.learn("date-time",diary_id,"datetime: " + datetime)
  context.learn("tags",diary_id,tags_sp)
  context.learn("author",diary_id,author)
  context.learn("title",diary_id,"text: " + title)
  context.learn("intro",diary_id,"text: " + intro)
  context.learn("body",diary_id,"text: " + body)
  context.learn("intro-wc",diary_id,"number: " + str(intro_wc))
  context.learn("body-wc",diary_id,"number: " + str(body_wc))
  context.learn("total-wc",diary_id,"number: " + str(total_wc))

# extract the comment details:
  element = soup('table',cellpadding='6',cellspacing='0',border='0')
  for line in element:
    parent_url = ""
    for a_link in line.find_all('table')[2].find_all('a'):
      if a_link.text == "Parent":
        parent_url = a_link['href']                             

# don't want "break" here, since want to match last one in the post.

#    body = line.find_all('table')[2].get_text().encode('ascii', 'ignore').decode('ascii').replace('\n','\\n').replace('\r','').replace('[  Parent  ] ','')
    body = chomp_bad_chars(line.find_all('table')[2].get_text()).replace('[  Parent  ] ','')
    wc = len(body.split())
    datetime = line.find_all('font')[1].contents[2][4:]
    kuron = chomp_bad_chars(line.find_all('font')[1].a.text)
    relative_url = line.font.find_all('a')[1]['href']
    comment_id = "comment: " + "-".join(relative_url[10:].split('#')[0].split('/'))
    parent_id = "-".join(parent_url[10:].split('#')[0].split('/'))
#    comment_title = line.font.b.text.encode('ascii', 'ignore').decode('ascii')
    comment_title = chomp_bad_chars(line.font.b.text)

# now some tidying up:
    url = "http://www.kuro5hin.org" + relative_url
    author = "kuron: " + kuron
    if parent_url == "":
      is_top_level_comment = "yes"
      parent_comment = ""
    else:
      is_top_level_comment = "no"
      parent_comment = "comment: " + parent_id

# now learn it all:
    context.add_learn("child-comment",diary_id,comment_id)
    context.learn("parent-diary",comment_id,diary_id)
    context.learn("url",comment_id,"url: " + url)
    context.learn("date-time",comment_id,"datetime: " + datetime)
    context.learn("author",comment_id,author)
    context.learn("title",comment_id,"text: " + comment_title)
    context.learn("body",comment_id,"text: " + body)
    context.learn("wc",comment_id,"number: " + str(wc))
    context.learn("is-top-level-comment",comment_id,is_top_level_comment)
    context.learn("parent-comment",comment_id,parent_comment)


# set learn k5id data context:
#C.set("k5id data")
# load current k5id data:
#load_sw(C,k5id_file)            # a note: should it be load_sw(C,file), or C.load_sw(file)?
#load_sw(C,k5id_file)

###
for k in range( hi, lo, -1):
  url = "http://www.kuro5hin.org/section/" + s_name +"/" + str(k-1)
  print("url:",url)
  diary_file = tmp_destination + "/test"+ s_name+ "-" + str(k-1) + ".html"
  diary = download_html(url)
  f = open(diary_file,'wb')      # hrmm... do we really need to keep copies of these??
  f.write(diary)
  f.close()

# extract k5id data:
  soup = BeautifulSoup(diary,"html.parser")
  element = soup('font',face="arial, Helvetica, Sans-Serif",size='2')
  for line in element:
    try:
# extract our variables:
      relative_url = line.contents[0].get('href')
      if relative_url.startswith('/story/'):
        download_url = False
        url = "http://www.kuro5hin.org" + relative_url
        k5id = "diary: " + "-".join(relative_url[7:].split('/'))
        print(k5id)
        comment_line = str(line.contents[1])
        if not " comment" in comment_line:
          comments = "0"
        else:
          comments = comment_line.split('(')[1].split()[0]
        print("comments:",comments)

# recall the old comment count:
        old_comments = C.recall("how-many-comments",k5id).apply_fn(extract_value).the_label()
        if old_comments == "":
          download_url = True
          old_comments = "0"
        print("old comments:",old_comments)

# decide if we want to download the given diary:
        if int(comments) > int(old_comments):
          download_url = True
#


# download and process the diary:
        if download_url:
          working_diary = download_diary(url,k5id[7:])
          process_diary(C,working_diary)

	  
# learn the data:
        C.learn("url",k5id,"url: " + url)
        C.learn("how-many-comments",k5id,"number: " + comments)
        soup = BeautifulSoup(working_diary, "html.parser")
        for i in soup.find_all('a'):
          try:
            relative_url = i.get('href')
            if relative_url.startswith('/poll/'):
              pollid = "poll : " + relative_url[6:]
              print(pollid)
              print(relative_url)
              url = "http://www.kuro5hin.org" + relative_url
              working_poll = download_diary(url,pollid[7:]) 
              C.learn("link-to-poll", k5id +" : "+ pollid, "yes")
#             process_poll(C,working_poll)
          except:
            continue
        

### download and learn poll result

    except:
      continue

##  sys.stdout.flush()
##  time.sleep(sleep_time)
#  break

##print(C.dump_universe())  
#save_sw(C,k5id_file,False)
save_sw(C,k5_file,False)

#sys.exit(0)
# write out the new download tally:
f = open(download_tally_file,mode='w')
f.write(str(download_tally))
f.close()

