#!/usr/bin/env python
# coding: utf-8

# In[93]:


import matplotlib.pyplot as plt
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
get_ipython().run_line_magic('matplotlib', 'inline')

# work in offline
# import plotly.offline as pyo
# pyo.init_notebook_mode()

# graph renderer
import plotly.io as pio
png_renderer = pio.renderers["png"]
# png_renderer.width=1800
# png_renderer.height=1200
# png_renderer.autoscale=True
pio.renderers.default = "png"

plotly.__version__


# In[94]:


def latency_df(result_dir, dir_prefix):
    latency_data = pd.read_csv(os.path.join(result_dir, dir_prefix, "count-latency.txt"),
                               delimiter=" ", header=None, 
                               names = "count latency currTime subTask".split())
    latency_data = latency_data[latency_data["latency"] > 0] # filter out outliers
    minTime = latency_data["currTime"].min()
    latency_data["time"] = latency_data["currTime"] - minTime
    #latency_data["time"] = latency_data["currTime"] - latency_data["latency"] - minTime
    #latency_data = latency_data[latency_data["time"] > 0]
    latency_data.sort_values("time", inplace=True)
    return latency_data

def failure_df(result_dir, dir_prefix, minTime):
    failure_data = pd.read_csv(os.path.join(result_dir, dir_prefix, "restart-cost.txt"),
                               delimiter=" ", skiprows=[0])
    failure_data["failedTimeFromZero"] = failure_data["failedTime"] - minTime
    failure_data["RecoveryLength_ms"] = failure_data["loadCheckpointCompleteTime"] - failure_data["RecoveryStartTime"]
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

def resource_df(result_dir, dir_prefix, resource, host, minTime):
    resource_data = pd.read_csv(os.path.join(result_dir, dir_prefix, resource + "-" + host + ".txt"),
                               delimiter=" ", error_bad_lines=False)
    resource_data["timeFromZero"] = resource_data["timestamp"] - minTime
    resource_data.sort_values("timeFromZero", inplace=True)
    return resource_data


# In[95]:


# TRIM_BEGIN = 0
# TRIM_END = -1
# def latency_throughput_plot(result_dir, dir_prefix):
#     latency_data = latency_df(result_dir, dir_prefix)[TRIM_BEGIN:TRIM_END]
#     minTime = latency_data["currTime"].min()
#     if minTime is np.nan:
#         minTime = 0
#     failure_data = failure_df(result_dir, dir_prefix, minTime)
#     checkpoint_data = checkpoint_df(result_dir, dir_prefix, minTime)
    
#     tm_list = ["flink2", "flink3", "redis2"]
#     throughput_data_list = [(tm, throughput_df(result_dir, dir_prefix, tm + ".txt", minTime))
#                             for tm in tm_list]

#     # Create figure with secondary y-axis
#     fig = make_subplots(specs=[[{"secondary_y": True}]])
#     #general layout
#     fig.update_layout(
#         title=dir_prefix,               # 标题文本 不设置位置的话 默认在左上角，下面有设置位置和颜色的讲解
# #         width=1600,
# #         height=980,
#         xaxis_title="Output Timestamp (ms)",       # X轴标题文本 
#         yaxis_title="Latency (ms)",       # Y轴标题文本    
#         legend_title="",      # 图例标题文本
#         font=dict(
#             family="Courier New, monospace", # 所有标题文字的字体
#             size=16,                         # 所有标题文字的大小
#             color="RebeccaPurple"            # 所有标题的颜色      
#         ),
#         xaxis_title_font_family='Times New Roman', # 额外设置x轴标题的字体
#         yaxis_title_font_color = 'red'             # 额外将y轴的字体设置为红色  
#     )
#     #secondary y axis
#     fig.update_yaxes(title_text="Throughput (elements/s/core)", secondary_y=True)
#     #legend
#     fig.update_layout(legend=dict(
#         orientation="h",
#         yanchor="top", xanchor="center",
#         x=0.5, y=-0.2
#     ))
    
#     #latency
#     fig.add_trace(go.Scatter(
#         x = latency_data.time,
#         y = latency_data.latency, 
#         mode="markers", marker=dict(size=3),
#         name = "latency"
#     ))
    
#     # failures
#     recovered_failure = failure_data[(failure_data["loadCheckpointCompleteTime"] > 0) & (failure_data["RecoveryStartTime"] > 0)]
#     for i, (start, cost) in enumerate(zip(recovered_failure["failedTimeFromZero"], recovered_failure["RecoveryLength_ms"])):
#         fig.add_trace(
#             go.Scatter(x=[start, start + cost],
#                        y=[0, 0],
#                        name="Recovered Failure",
#                        legendgroup="Recovered Failure",
#                        marker=dict(symbol="x",size=10,color="rgba(255, 0, 0, 0.5)",line=dict(color="red")),
#                        showlegend= i == 0))

#     unrecovered_failure = failure_data[(failure_data["loadCheckpointCompleteTime"] <= 0) | (failure_data["RecoveryStartTime"] <= 0)]
#     failure_y = np.empty(len(unrecovered_failure))
#     failure_y.fill(0)
#     fig.add_trace(go.Scatter(
#         x = unrecovered_failure["failedTimeFromZero"],
#         y = failure_y,
#         mode="markers", 
#         marker=dict(color="red", symbol="x-thin",size=10, line=dict(color="red", width=1)),
#         legendgroup="Unrecovered Failure",
#         name = "Unrecovered Failure"
#     ))
    
#     # checkpoints
#     successful_checkpoints = checkpoint_data[checkpoint_data["size_bytes"] > 0]
#     for i, (start, cost) in enumerate(zip(successful_checkpoints["startTimeFromZero"],successful_checkpoints["timeCost_ms"])):
#         fig.add_trace(
#             go.Scatter(x=[start, start + cost],
#                        y=[0, 0],
#                        name="Successful Checkpoint",
#                        legendgroup="Successful Checkpoint",
#                        marker=dict(symbol='line-ns', color="green", size=7, line=dict(color="green", width=1)),
#                        showlegend= i == 0))
        
#     failed_checkpoints = checkpoint_data[checkpoint_data["size_bytes"] == 0]
#     for i, (start, cost) in enumerate(zip(failed_checkpoints["startTimeFromZero"],failed_checkpoints["timeCost_ms"])):
#         fig.add_trace(
#             go.Scatter(x=[start, start + cost],
#                        y=[0, 0],
#                        name="Failed Checkpoint",
#                        legendgroup="Failed Checkpoint",
#                        marker=dict(symbol='line-ns', color="red", size=7, line=dict(color="red", width=1)),
#                        showlegend= i == 0))

# #     fig.add_trace(go.Scatter(
# #         x = checkpoint_data["startTimeFromZero"],
# #         y = np.zeros(len(checkpoint_data)),
# #         mode="markers", marker_symbol="square-open",
# #         name = "checkpoint"
# #     ))

# #     throughput_y = np.empty(len(throughput_data))
# #     throughput_y.fill(1000)
#     for (tm, throughput_data) in throughput_data_list:
#         fig.add_trace(go.Scatter(
#             x = throughput_data["startTimeFromZero"],
#             y = throughput_data["elements/second/core"], 
#             mode="markers", marker_symbol="triangle-up",
#             name = tm + "_throughput"
#         ), secondary_y=True)

#     fig.show(scale=2)


# SAMPLE_RATE_S = 2
# def resource_plot(result_dir, dir_prefix, hosts):
#     latency_data = latency_df(result_dir, dir_prefix)[TRIM_BEGIN:TRIM_END]
#     minTime = latency_data["currTime"].min()
#     if minTime is np.nan:
#         minTime = 0
#     failure_data = failure_df(result_dir, dir_prefix, minTime)
#     checkpoint_data = checkpoint_df(result_dir, dir_prefix, minTime)
    
#     for host in hosts:
#         # Create figure with secondary y-axis
#         #fig = make_subplots(specs=[[{"secondary_y": True}]])
#         fig = make_subplots(
#             rows=2, cols=1,
#             subplot_titles=("CPU/Memory Usage Over Time", "Dist/Network Usage Over Time"))
#         #top
#         fig.update_yaxes(title_text="CPU/Memory Usage(%)", title_font = {"size": 15}, row=1, col=1)
#         fig.update_xaxes(title_text="timestamp (ms)", title_font = {"size": 15}, row=1, col=1)
#         #bottom
#         fig.update_yaxes(title_text="Disk/Network Usage (Bytes/s)", title_font = {"size": 15}, row=2, col=1)
#         fig.update_xaxes(title_text="timestamp (ms)", title_font = {"size": 15}, row=2, col=1)

#         #general layout
#         fig.update_layout(
#             title=dir_prefix + "-" + host,               # 标题文本 不设置位置的话 默认在左上角，下面有设置位置和颜色的讲解
#             font=dict(
#                 family="Courier New, monospace", # 所有标题文字的字体
#                 size=16,                         # 所有标题文字的大小
#                 color="RebeccaPurple"            # 所有标题的颜色      
#             ),
#             height=700
#         )
#         #legend
#         fig.update_layout(legend=dict(
#             orientation="h",
#             yanchor="top", xanchor="center",
#             x=0.5, y=-0.2
#         ))
        
#         #cpu
#         cpu = resource_df(result_dir, dir_prefix, "cpu", host, minTime)
#         cpu["used"] = 100 - cpu["idle"]
#         fig.add_trace(go.Scatter(
#             x = cpu.timeFromZero,
#             y = cpu["used"],
# #             line=dict(dash = 'dot'),
#             name = "CPU Usage"
#         ), row=1, col=1)
        
#         #memory
#         memory = resource_df(result_dir, dir_prefix, "memory", host, minTime)
# #         memory["used_percent"] = (1 - memory["free"] / memory["total"]) * 100
#         memory["used_percent"] = (memory["used"] / memory["total"]) * 100
#         fig.add_trace(go.Scatter(
#             x = memory.timeFromZero,
#             y = memory["used_percent"],
#             line=dict(dash = 'dash'),
#             name = "Memory Usage"
#         ), row=1, col=1)
        
#         #network
#         network = resource_df(result_dir, dir_prefix, "network", host, minTime)
#         network = network.drop(columns=["interface", "timestamp"])
#         # calculate diff between consecutive rows except for timestamp
#         network = network.set_index("timeFromZero").diff() / SAMPLE_RATE_S 
#         fig.add_trace(go.Scatter(
#             x = network.index,
#             y = network["recv_bytes"],
#             line=dict(width=1),
#             name = "Network Read"
#         ), row=2, col=1)
#         fig.add_trace(go.Scatter(
#             x = network.index,
#             y = network["sent_bytes"],
#             line=dict(width=1),
#             name = "Network Write"
#         ), row=2, col=1)

#         #disk
#         # timestamp disk read_total read_merged read_sectors read_ms write_total write_merged write_sectors write_ms io_cur io_sec
#         disk = resource_df(result_dir, dir_prefix, "disk", host, minTime)
#         disk = disk.drop(columns=["disk", "timestamp"])
#         disk = disk.set_index("timeFromZero").diff() / SAMPLE_RATE_S
#         # Sector is 512 bytes
#         disk["total_read_bytes_per_s"] = disk["read_sectors"] * 512
#         disk["total_write_bytes_per_s"] = disk["write_sectors"] * 512
#         fig.add_trace(go.Scatter(
#             x = disk.index,
#             y = disk["total_read_bytes_per_s"],
#             line=dict(dash = 'dashdot'),
#             name = "Disk Read"
#         ), row=2, col=1)
#         fig.add_trace(go.Scatter(
#             x = disk.index,
#             y = disk["total_write_bytes_per_s"],
#             line=dict(dash = 'dashdot'),
#             name = "Disk Write"
#         ), row=2, col=1)
        
#         # failures
#         recovered_failure = failure_data[(failure_data["loadCheckpointCompleteTime"] > 0) & (failure_data["RecoveryStartTime"] > 0)]
#         for i, (start, cost) in enumerate(zip(recovered_failure["failedTimeFromZero"], recovered_failure["RecoveryLength_ms"])):
#             fig.add_trace(
#                 go.Scatter(x=[start, start + cost],
#                            y=[0, 0],
#                            name="Recovered Failure",
#                            legendgroup="Recovered Failure",
#                            marker=dict(symbol="x",size=10,color="red",line=dict(color="red")),
#                            showlegend= i == 0),
#                 row=1, col=1)
#             fig.add_trace(
#                 go.Scatter(x=[start, start + cost],
#                            y=[0, 0],
#                            name="Recovered Failure",
#                            legendgroup="Recovered Failure",
#                            marker=dict(symbol="x",size=10,color="red",line=dict(color="red")),
#                            showlegend=False),
#                 row=2, col=1)
            
#         unrecovered_failure = failure_data[(failure_data["loadCheckpointCompleteTime"] <= 0) | (failure_data["RecoveryStartTime"] <= 0)]
#         failure_y = np.empty(len(unrecovered_failure))
#         failure_y.fill(0)
#         fig.add_trace(go.Scatter(
#             x = unrecovered_failure["failedTimeFromZero"],
#             y = failure_y,
#             mode="markers", 
#             marker=dict(color="red", symbol="x-thin",size=10, line=dict(color="red", width=1)),
#             legendgroup="Unrecovered Failure",
#             name = "Unrecovered Failure"
#         ), row=1, col=1)
#         fig.add_trace(go.Scatter(
#             x = unrecovered_failure["failedTimeFromZero"],
#             y = failure_y,
#             mode="markers", 
#             marker=dict(color="red", symbol="x-thin",size=10, line=dict(color="red", width=1)),
#             legendgroup="Unrecovered Failure",
#             name = "Unrecovered Failure",
#             showlegend=False
#         ), row=2, col=1)

#         # checkpoints
#         successful_checkpoints = checkpoint_data[checkpoint_data["size_bytes"] > 0]
#         for i, (start, cost) in enumerate(zip(successful_checkpoints["startTimeFromZero"],successful_checkpoints["timeCost_ms"])):
#             fig.add_trace(
#                 go.Scatter(x=[start, start + cost],
#                            y=[0, 0],
#                            name="Successful Checkpoint",
#                            legendgroup="Successful Checkpoint",
#                            marker=dict(color="green", size=7, line=dict(color="green")),
#                            showlegend= i == 0),
#                 row=1, col=1)
#             fig.add_trace(
#                 go.Scatter(x=[start, start + cost],
#                            y=[0, 0],
#                            name="Successful Checkpoint",
#                            legendgroup="Successful Checkpoint",
#                            marker=dict(color="green", size=7, line=dict(color="green")),
#                            showlegend=False),
#                 row=2, col=1)
        
#         failed_checkpoints = checkpoint_data[checkpoint_data["size_bytes"] == 0]
#         for i, (start, cost) in enumerate(zip(failed_checkpoints["startTimeFromZero"],failed_checkpoints["timeCost_ms"])):
#             fig.add_trace(
#                 go.Scatter(x=[start, start + cost],
#                            y=[0, 0],
#                            name="Failed Checkpoint",
#                            legendgroup="Failed Checkpoint",
#                            marker=dict(color="red", size=7, line=dict(color="red")),
#                            showlegend= i == 0),
#                 row=1, col=1)
#             fig.add_trace(
#                 go.Scatter(x=[start, start + cost],
#                            y=[0, 0],
#                            name="Failed Checkpoint",
#                            legendgroup="Failed Checkpoint",
#                            marker=dict(color="red", size=7, line=dict(color="red")),
#                            showlegend=False),
#                 row=2, col=1)
            
#         fig.show(scale=2)


# In[96]:


def latency_plot(latency_data):
    return go.Scatter(
        x = latency_data.time,
        y = latency_data.latency, 
        mode="markers", marker=dict(size=2),
        name = "latency")

throughput_colors = ['#B6E880', '#FF97FF', '#FECB52']
def throughput_plot(tm, throughput_data, color_idx):
    return go.Scatter(
        x = throughput_data["startTimeFromZero"],
        y = throughput_data["elements/second/core"], 
        mode="markers", marker=dict(symbol="triangle-up", color=throughput_colors[color_idx]),
        name = tm + "_throughput")
       
def cpu_usage_plot(cpu):
    return go.Scatter(
            x = cpu.timeFromZero,
            y = cpu["used"],
#             line=dict(dash = 'dot'),
            name = "CPU Usage"
        )

def memory_usage_plot(memory):
    return go.Scatter(
            x = memory.timeFromZero,
            y = memory["used_percent"],
            line=dict(dash = 'dash'),
            name = "Memory Usage"
        )

def network_read_plot(network):
    return go.Scatter(
            x = network.index,
            y = network["recv_bytes"],
            line=dict(width=1),
            name = "Network Read"
        )
def network_write_plot(network):    
    return go.Scatter(
            x = network.index,
            y = network["sent_bytes"],
            line=dict(width=1),
            name = "Network Write"
        )

def disk_read_plot(disk):    
    return go.Scatter(
            x = disk.index,
            y = disk["total_read_bytes_per_s"],
            line=dict(dash = 'dashdot'),
            name = "Disk Read"
        )
def disk_write_plot(disk):
    return go.Scatter(
            x = disk.index,
            y = disk["total_write_bytes_per_s"],
            line=dict(dash = 'dashdot'),
            name = "Disk Write"
        )

def recovered_failure_plot(recovered_failure, showlegend):
    res = []
    for i, (start, cost) in enumerate(zip(recovered_failure["failedTimeFromZero"], recovered_failure["RecoveryLength_ms"])):
        res.append(go.Scatter(x=[start, start + cost],
                   y=[0, 0],
                   name="Recovered Failure",
                   legendgroup="Recovered Failure",
                   marker=dict(symbol="x",size=7,color="rgba(255, 0, 0, 1)"),
#                    marker=dict(symbol="x",size=7,color="rgba(255, 0, 0, 1)",line=dict(color="red", width=2)),
               showlegend= i == 0 if showlegend else False))        
    return res
def unrecovered_failure_plot(unrecovered_failure, showlegend):
    failure_y = np.empty(len(unrecovered_failure))
    failure_y.fill(0)
    return go.Scatter(
            x = unrecovered_failure["failedTimeFromZero"],
            y = failure_y,
            mode="markers", 
            marker=dict(color="red", symbol="x-thin",size=10, line=dict(color="red", width=1)),
            legendgroup="Unrecovered Failure",
            name = "Unrecovered Failure",
            showlegend=showlegend
        )

def successful_checkpoints_plot(successful_checkpoints, showlegend):
    res = []
    for i, (start, cost) in enumerate(zip(successful_checkpoints["startTimeFromZero"],successful_checkpoints["timeCost_ms"])):
        res.append(go.Scatter(
            x=[start, start + cost],
            y=[0, 0],
            name="Successful Checkpoint",
            legendgroup="Successful Checkpoint",
            marker=dict(symbol='line-ns', color="green", size=7, line=dict(color="green", width=1)),
            showlegend= i == 0 if showlegend else False))        
    return res
def failed_checkpoints_plot(successful_checkpoints, showlegend):
    res = []
    for i, (start, cost) in enumerate(zip(successful_checkpoints["startTimeFromZero"],successful_checkpoints["timeCost_ms"])):
        res.append(
            go.Scatter(x=[start, start + cost],
            y=[0, 0],
            name="Failed Checkpoint",
            legendgroup="Failed Checkpoint",
            mode="markers",
            marker=dict(color="rgba(0,0,0,0)", size=7, line=dict(color="red", width=1)),
            showlegend= i == 0 if showlegend else False))        
    return res


# In[97]:


TRIM_BEGIN = 0
TRIM_END = -1
def latency_throughput_plot(result_dir, dir_prefix):
    latency_data = latency_df(result_dir, dir_prefix)[TRIM_BEGIN:TRIM_END]
    minTime = latency_data["currTime"].min()
    if minTime is np.nan:
        minTime = 0
    failure_data = failure_df(result_dir, dir_prefix, minTime)
    checkpoint_data = checkpoint_df(result_dir, dir_prefix, minTime)
    
    tm_list = ["flink2", "flink3", "redis2"]
    throughput_data_list = [(tm, throughput_df(result_dir, dir_prefix, tm + ".txt", minTime))
                            for tm in tm_list]

    # Create figure with secondary y-axis
    fig = make_subplots(specs=[[{"secondary_y": True}]])
    #general layout
    fig.update_layout(
        title=dir_prefix,               # 标题文本 不设置位置的话 默认在左上角，下面有设置位置和颜色的讲解
#         width=1600,
#         height=980,
        xaxis_title="Output Timestamp (ms)",       # X轴标题文本 
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
        yanchor="top", xanchor="center",
        x=0.5, y=-0.2
    ))
    
    #latency
    fig.add_trace(latency_plot(latency_data))
       
    # throughputs
    for idx, (tm, throughput_data) in enumerate(throughput_data_list):
        fig.add_trace(throughput_plot(tm, throughput_data, idx), secondary_y=True)

    # failures
    recovered_failure = failure_data[(failure_data["loadCheckpointCompleteTime"] > 0) & (failure_data["RecoveryStartTime"] > 0)]
    fig.add_traces(recovered_failure_plot(recovered_failure, True))

    unrecovered_failure = failure_data[(failure_data["loadCheckpointCompleteTime"] <= 0) | (failure_data["RecoveryStartTime"] <= 0)]
    fig.add_trace(unrecovered_failure_plot(unrecovered_failure, True))

    # checkpoints
    successful_checkpoints = checkpoint_data[checkpoint_data["size_bytes"] > 0]
    fig.add_traces(successful_checkpoints_plot(successful_checkpoints, True))

    failed_checkpoints = checkpoint_data[checkpoint_data["size_bytes"] == 0]
    fig.add_traces(failed_checkpoints_plot(failed_checkpoints, True))

    fig.show(scale=2) 

SAMPLE_RATE_S = 2
def resource_plot(result_dir, dir_prefix, host):
    latency_data = latency_df(result_dir, dir_prefix)[TRIM_BEGIN:TRIM_END]
    minTime = latency_data["currTime"].min()
    if minTime is np.nan:
        minTime = 0
    failure_data = failure_df(result_dir, dir_prefix, minTime)
    checkpoint_data = checkpoint_df(result_dir, dir_prefix, minTime)
    
    # Create figure with secondary y-axis
    #fig = make_subplots(specs=[[{"secondary_y": True}]])
    fig = make_subplots(
        rows=2, cols=1,
        subplot_titles=("CPU/Memory Usage Over Time", "Dist/Network Usage Over Time"))
    #top
    fig.update_yaxes(title_text="CPU/Memory Usage(%)", title_font = {"size": 15}, row=1, col=1)
    fig.update_xaxes(title_text="timestamp (ms)", title_font = {"size": 15}, row=1, col=1)
    #bottom
    fig.update_yaxes(title_text="Disk/Network Usage (Bytes/s)", title_font = {"size": 15}, row=2, col=1)
    fig.update_xaxes(title_text="timestamp (ms)", title_font = {"size": 15}, row=2, col=1)

    #general layout
    fig.update_layout(
        title=dir_prefix + "-" + host,               # 标题文本 不设置位置的话 默认在左上角，下面有设置位置和颜色的讲解
        font=dict(
            family="Courier New, monospace", # 所有标题文字的字体
            size=16,                         # 所有标题文字的大小
            color="RebeccaPurple"            # 所有标题的颜色      
        ),
        height=700
    )
    #legend
    fig.update_layout(legend=dict(
        orientation="h",
        yanchor="top", xanchor="center",
        x=0.5, y=-0.2
    ))

    #cpu
    cpu = resource_df(result_dir, dir_prefix, "cpu", host, minTime)
    cpu["used"] = 100 - cpu["idle"]
    fig.add_trace(cpu_usage_plot(cpu), row=1, col=1)

    #memory
    memory = resource_df(result_dir, dir_prefix, "memory", host, minTime)
#         memory["used_percent"] = (1 - memory["free"] / memory["total"]) * 100
    memory["used_percent"] = (memory["used"] / memory["total"]) * 100
    fig.add_trace(memory_usage_plot(memory), row=1, col=1)

    #network
    network = resource_df(result_dir, dir_prefix, "network", host, minTime)
    network = network.drop(columns=["interface", "timestamp"])
    # calculate diff between consecutive rows except for timestamp
    network = network.set_index("timeFromZero").diff() / SAMPLE_RATE_S 
    fig.add_trace(network_read_plot(network), row=2, col=1)
    fig.add_trace(network_write_plot(network), row=2, col=1)

    #disk
    # timestamp disk read_total read_merged read_sectors read_ms write_total write_merged write_sectors write_ms io_cur io_sec
    disk = resource_df(result_dir, dir_prefix, "disk", host, minTime)
    disk = disk.drop(columns=["disk", "timestamp"])
    disk = disk.set_index("timeFromZero").diff() / SAMPLE_RATE_S
    # Sector is 512 bytes
    disk["total_read_bytes_per_s"] = disk["read_sectors"] * 512
    disk["total_write_bytes_per_s"] = disk["write_sectors"] * 512
    fig.add_trace(disk_read_plot(disk), row=2, col=1)
    fig.add_trace(disk_write_plot(disk), row=2, col=1)

    # failures
    recovered_failure = failure_data[(failure_data["loadCheckpointCompleteTime"] > 0) & (failure_data["RecoveryStartTime"] > 0)]
    ones = [1] * len(recovered_failure)
    twos = [2] * len(recovered_failure)
    fig.add_traces(recovered_failure_plot(recovered_failure, True), rows=ones, cols=ones)
    fig.add_traces(recovered_failure_plot(recovered_failure, False), rows=twos, cols=ones)

    unrecovered_failure = failure_data[(failure_data["loadCheckpointCompleteTime"] <= 0) | (failure_data["RecoveryStartTime"] <= 0)]
    fig.add_trace(unrecovered_failure_plot(unrecovered_failure, True), row=1, col=1)
    fig.add_trace(unrecovered_failure_plot(unrecovered_failure, False), row=2, col=1)

    # checkpoints
    successful_checkpoints = checkpoint_data[checkpoint_data["size_bytes"] > 0]
    ones = [1] * len(successful_checkpoints)
    twos = [2] * len(successful_checkpoints)
    fig.add_traces(successful_checkpoints_plot(successful_checkpoints, True), rows=ones, cols=ones)
    fig.add_traces(successful_checkpoints_plot(successful_checkpoints, False), rows=ones, cols=ones)

    failed_checkpoints = checkpoint_data[checkpoint_data["size_bytes"] == 0]
    ones = [1] * len(failed_checkpoints)
    twos = [2] * len(failed_checkpoints)
    fig.add_traces(failed_checkpoints_plot(failed_checkpoints, True), rows=ones, cols=ones)
    fig.add_traces(failed_checkpoints_plot(failed_checkpoints, False), rows=ones, cols=ones)
        
    fig.show(scale=2) 


# In[98]:


result_dir = "C:\\Users\\joinp\\Downloads\\results\\"
# result_dir = "C:\\Wenzhong\\我的坚果云\\实验\\results\\multilevel"
# result_dir = "C:\\Wenzhong\\我的坚果云\\实验\\results\\singlelevel"
dirs = os.listdir(result_dir)
[path for path in dirs if os.path.isdir(path)]


# In[104]:


# draw all
result_dir="C:\\Users\\joinp\\Downloads\\results\\"
hosts=["flink2","flink3"]
# hosts=["hadoop1","hadoop2","hadoop3","hadoop4","flink1","flink2","flink3","kafka1","kafka2","redis1","redis2","zk1"]
for dir_prefix in os.listdir(result_dir):
    if os.path.isdir(result_dir+dir_prefix):
        latency_throughput_plot(os.path.abspath(result_dir), dir_prefix)
        for host in hosts:
            resource_plot(result_dir, dir_prefix, host)


# In[77]:


# result_dir="C:\\Users\\joinp\\Downloads\\results\\"
# dir_prefix="09-06_08-01-22_load-140000-multi"
# latency_data = latency_df(result_dir, dir_prefix)[TRIM_BEGIN:TRIM_END]
# minTime = latency_data["currTime"].min()
# if minTime is np.nan:
#     minTime = 0
# failure_data = failure_df(result_dir, dir_prefix, minTime)
# checkpoint_data = checkpoint_df(result_dir, dir_prefix, minTime)
# host="flink1"
# #cpu
# cpu = resource_df(result_dir, dir_prefix, "cpu", host, minTime)
# cpu["used"] = 100 - cpu["idle"]
# cpu


# In[78]:


import time, datetime
# 2021-06-27 04:29:27,067 UTC 1624768167067

t = time.localtime(1624768167067/1000)
# datetime.utcfromtimestamp(t)
time.strftime("%Y-%m-%d %H:%M:%S %Z", time.gmtime(1624768167067/1000))


# In[79]:


time.gmtime(1624768167067/1000)


# In[80]:


# latency_data = latency_df("C:\\Users\\joinp\\Downloads\\results\\", "08-31_12-57-17_load-200000")
# minTime = latency_data["currTime"].min()
# checkpoint_data = checkpoint_df("C:\\Users\\joinp\\Downloads\\results\\", "08-31_12-57-17_load-200000", minTime)
# checkpoint_data.head()


# In[81]:


#  for i, (start, end) in enumerate(zip(checkpoint_data["startTimeFromZero"],checkpoint_data["timeCost_ms"])):
#         print(start, start + end)


# In[82]:


def recover_cost(result_dir, dir_prefix, minTime):
    checkpoint_data = checkpoint_df(result_dir, dir_prefix, minTime)
    checkpoint_data = checkpoint_data[checkpoint_data["size_bytes"] > 0]

    failure_data = failure_df(result_dir, dir_prefix, minTime)
    failure_data = failure_data[failure_data["checkpointId"] > 0]
    return pd.merge(left=failure_data, right=checkpoint_data[["id", "size_bytes"]], how='left',
                   left_on='checkpointId', right_on='id')

# result_dir="C:\\Users\\joinp\\Downloads\\results\\"
# dir_prefix="09-07_19-46-32_load-80000-single"

# latency_data = latency_df(result_dir, dir_prefix)
# minTime = latency_data["currTime"].min()

# failure_data = recover_cost(result_dir, dir_prefix, minTime)
# print(failure_data[["checkpointId","RecoveryLength_ms", "size_bytes"]])


# In[83]:


result_dir="C:\\Users\\joinp\\Downloads\\results\\"

for dir_prefix in os.listdir(result_dir):
    if os.path.isdir(result_dir+dir_prefix):
        latency_data = latency_df(result_dir, dir_prefix)
        minTime = latency_data["currTime"].min()

        failure_data = recover_cost(result_dir, dir_prefix, minTime)
        print(dir_prefix)
        print(failure_data[["checkpointId","RecoveryLength_ms", "size_bytes"]].head())


# In[84]:


# result_dir="C:\\Users\\joinp\\Downloads\\results\\"
# dir_prefix="09-05_10-53-19_load-170000-single"
# latency_data = latency_df(result_dir, dir_prefix)[TRIM_BEGIN:TRIM_END]
# minTime = latency_data["currTime"].min()

# cpu = resource_df(result_dir, dir_prefix, "cpu", "zk1", minTime)

# cpu["used"] = 100 - cpu["idle"]
# data = []
# data.append(go.Scatter(
#     x = cpu.timeFromZero,
#     y = cpu["used"],
#     name = "CPU Usage"
# ))

# fig = go.Figure(
#     data=data,
#     layout=go.Layout(title=go.layout.Title(text="CPU Usage Over Time",x=0.5),
#         xaxis={'title':'Time(ms)'},
#         yaxis={'title':'CPU Usage(%)'}))
# fig.show()


# In[85]:


# result_dir="C:\\Users\\joinp\\Downloads\\results\\"
# dir_prefix="09-05_15-03-49_load-160000-multi"
# latency_data = latency_df(result_dir, dir_prefix)[TRIM_BEGIN:TRIM_END]
# minTime = latency_data["currTime"].min()

# memory = resource_df(result_dir, dir_prefix, "memory", "zk1", minTime)
# memory["used_percent"] = (1 - memory["free"] / memory["total"]) * 100
# memory.head()

# # cpu["used"] = 100 - cpu["idle"]
# # data = []
# # data.append(go.Scatter(
# #     x = cpu.timeFromZero,
# #     y = cpu["used"],
# #     name = "CPU Usage"
# # ))

# # fig = go.Figure(
# #     data=data,
# #     layout=go.Layout(title=go.layout.Title(text="CPU Usage Over Time",x=0.5),
# #         xaxis={'title':'Time(ms)'},
# #         yaxis={'title':'CPU Usage(%)'}))
# # fig.show()


# In[86]:


result_dir="C:\\Users\\joinp\\Downloads\\results\\"
dir_prefix="09-06_01-04-40_load-160000-single"
latency_data = latency_df(result_dir, dir_prefix)[TRIM_BEGIN:TRIM_END]
minTime = latency_data["currTime"].min()

a = 2
network = resource_df(result_dir, dir_prefix, "network", "zk1", minTime)
del network["interface"]
network = network.set_index("timestamp").diff() / a
network.head()


# In[ ]:




