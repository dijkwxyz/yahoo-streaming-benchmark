#!/usr/bin/env python
# coding: utf-8

# In[39]:


import matplotlib.pyplot as plt
get_ipython().run_line_magic('matplotlib', 'inline')
import plotly
import plotly.graph_objs as go
from plotly.subplots import make_subplots
from plotly.offline import init_notebook_mode,iplot
init_notebook_mode(connected=True)
import warnings
warnings.filterwarnings('ignore')
import numpy as np
import pandas as pd
import csv
import os

plotly.__version__


# In[ ]:


os.listdir("C:\\Users\\46522\\Downloads\\results\\")


# In[ ]:


result_dir = os.path.abspath("C:\\Users\\46522\\Downloads\\results\\06-26_20-07-55_load-200000")


# In[70]:


def latency_df(result_dir, dir_prefix):
    latency_data = pd.read_csv(os.path.join(result_dir, dir_prefix, "count-latency.txt"),
                               delimiter=" ", header=None, 
                               names = "count latency currTime subTask".split())
    minTime = latency_data["currTime"].min()
    latency_data["time"] = latency_data["currTime"] - minTime
    latency_data.sort_values("time", inplace=True)
    return latency_data

def failure_df(result_dir, dir_prefix, minTime):
    failure_data = pd.read_csv(os.path.join(result_dir, dir_prefix, "restart-cost.txt"),
                               delimiter=" ", skiprows=[0])
    failure_data["failedTimeFromZero"] = failure_data["failedTime"] - minTime
    failure_data.sort_values("failedTimeFromZero", inplace=True)
    return failure_data

def throughput_df(result_dir, dir_prefix, filename, minTime):
    throughput_data = pd.read_csv(os.path.join(result_dir, dir_prefix, filename),
                           delimiter=",")
    throughput_data["startTimeFromZero"] = throughput_data["start"] - minTime
    throughput_data.sort_values("startTimeFromZero", inplace=True)
    return throughput_data

def checkpoint_df(result_dir, dir_prefix, minTime):
    checkpoint_data = pd.read_csv(os.path.join(result_dir, dir_prefix, "checkpoints.txt"),
                           delimiter=" ")
    checkpoint_data["startTimeFromZero"] = checkpoint_data["startTime_ms"] - minTime
    checkpoint_data.sort_values("startTimeFromZero", inplace=True)
    return checkpoint_data


# In[ ]:





# In[71]:


import time, datetime
time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(1624760808457/1000)) # localtime not UTC


# In[72]:


result_dir
dir_prefix


# In[82]:


def make_plot(result_dir, dir_prefix):
    latency_data = latency_df(result_dir, dir_prefix)
    minTime = latency_data["currTime"].min()

    failure_data = failure_df(result_dir, dir_prefix, minTime)
    checkpoint_data = checkpoint_df(result_dir, dir_prefix, minTime)
    
    tm_list = ["flink2", "flink3", "redis2"]
    throughput_data_list = [(tm, throughput_df(result_dir, dir_prefix, tm + ".txt", minTime))
                            for tm in tm_list]

    # Create figure with secondary y-axis
    fig = make_subplots(specs=[[{"secondary_y": True}]])
    
    fig.add_trace(go.Scatter(
        x = latency_data.time,
        y = latency_data.latency, 
        name = "latency"
    ))
    
    failure_y = np.empty(len(failure_data))
    failure_y.fill(latency_data["latency"].mean())
    fig.add_trace(go.Scatter(
        x = failure_data["failedTimeFromZero"],
        y = failure_y,
        mode="markers", marker_symbol="x", 
        name = "failure"
    ))
    
    fig.add_trace(go.Scatter(
        x = checkpoint_data["startTimeFromZero"],
        y = np.zeros(len(checkpoint_data)),
        mode="markers", marker_symbol="circle",
        name = "checkpoint"
    ))

#     throughput_y = np.empty(len(throughput_data))
#     throughput_y.fill(1000)
    for (tm, throughput_data) in throughput_data_list:
        fig.add_trace(go.Scatter(
            x = throughput_data["startTimeFromZero"],
            y = throughput_data["elements/second/core"], 
    #         mode="markers", marker_symbol="triangle-up",
            name = tm + "_throughput"
        ), secondary_y=True)

    #general layout
    fig.update_layout(
        title=dir_prefix,               # 标题文本 不设置位置的话 默认在左上角，下面有设置位置和颜色的讲解
        xaxis_title="Time stamp (ms)",       # X轴标题文本 
        yaxis_title="Latency (ms)",       # Y轴标题文本    
        legend_title="",      # 图例标题文本
        font=dict(
            family="Courier New, monospace", # 所有标题文字的字体
            size=16,                         # 所有标题文字的大小
            color="RebeccaPurple"            # 所有标题的颜色      
        ),
        xaxis_title_font_family='Times New Roman', # 额外设置x轴标题的字体
        yaxis_title_font_color = 'red'             # 额外将y轴的字体设置为红色  
    )
    #secondary y axis
    fig.update_yaxes(title_text="Throughput (elements/s/core)", secondary_y=True)
    #legend
    fig.update_layout(legend=dict(
        orientation="h",
        yanchor="bottom", xanchor="center",
        x=0.5, y=1
    ))

    fig.show()


# In[83]:


result_dir = "C:\\Users\\46522\\Downloads\\results\\"
dirs = os.listdir(result_dir)
del dirs[0]
for dir_prefix in dirs:
    make_plot(os.path.abspath(result_dir), dir_prefix)


# In[77]:


import time, datetime
# 2021-06-27 04:29:27,067 UTC 1624768167067

t = time.localtime(1624768167067/1000)
# datetime.utcfromtimestamp(t)
time.strftime("%Y-%m-%d %H:%M:%S %Z", time.gmtime(1624768167067/1000))


# In[69]:


time.gmtime(1624768167067/1000)


# In[28]:


make_plot("C:\\Users\\46522\\Downloads\\results\\", "06-27_07-18-35_load-200000")


# In[ ]:





# In[ ]:





# In[ ]:




