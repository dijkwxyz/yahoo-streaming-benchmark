#!/usr/bin/env python
# coding: utf-8

# In[1]:


from IPython.display import display, HTML
import matplotlib.pyplot as plt
import plotly
import plotly.graph_objs as go
from plotly.subplots import make_subplots
from plotly.offline import init_notebook_mode,iplot
import plotly.express as px
from kaleido.scopes.plotly import PlotlyScope
import re

init_notebook_mode(connected=True)
import warnings
warnings.filterwarnings('ignore')
import numpy as np
import pandas as pd
import csv
import os
get_ipython().run_line_magic('matplotlib', 'inline')

# work in offline
import plotly.offline as pyo
pyo.init_notebook_mode()

# graph renderer
import plotly.io as pio
png_renderer = pio.renderers["png"]
# png_renderer.width=1800
# png_renderer.height=1200
# png_renderer.autoscale=True
pio.renderers.default = "png"

scope = PlotlyScope()

plotly.__version__

FIGURE_DIR = "C:\\Users\\joinp\\Downloads\\Figures\\"


# In[2]:


def latency_df(result_dir, dir_prefix, minTime):
    latency_data = pd.read_csv(os.path.join(result_dir, dir_prefix, "count-latency.txt"),
                               delimiter=" ", header=None, index_col=False,
                               names = "count latency currTime subTask".split())
    latency_data["latency"][latency_data["latency"] < 0] = 0 # filter out outliers
    latency_data["time"] = latency_data["currTime"] - minTime
    #latency_data["time"] = latency_data["currTime"] - latency_data["latency"] - minTime
    latency_data = latency_data[latency_data["time"] >= TIME_BEGIN]
    latency_data.sort_values("time", inplace=True)
    return latency_data

def failure_df(result_dir, dir_prefix, minTime, length):
    failure_data = pd.read_csv(os.path.join(result_dir, dir_prefix, "restart-cost.txt"),
                               delimiter=" ", skiprows=[0], index_col=False)
    failure_data["failedTimeFromZero"] = failure_data["failedTime"] - minTime
    failure_data["recoveredTimeFromZero"] = failure_data["loadCheckpointCompleteTime"] - minTime
    failure_data["RecoveryLength_ms"] = failure_data["loadCheckpointCompleteTime"] - failure_data["RecoveryStartTime"]
    failure_data.sort_values("failedTimeFromZero", inplace=True)
    failure_data = failure_data[(failure_data["failedTimeFromZero"] >= TIME_BEGIN) & (failure_data["failedTimeFromZero"] <= length)]
    return failure_data

def throughput_df(result_dir, dir_prefix, filename, minTime, length):
    throughput_data = pd.read_csv(os.path.join(result_dir, dir_prefix, filename),
                           delimiter=",", index_col=False)
    throughput_data["startTimeFromZero"] = throughput_data["start"] - minTime
    throughput_data.sort_values("startTimeFromZero", inplace=True)
    throughput_data = throughput_data[(throughput_data["startTimeFromZero"] >= TIME_BEGIN) & (throughput_data["startTimeFromZero"] <= length)]
    return throughput_data

def checkpoint_df(result_dir, dir_prefix, minTime, length):
    checkpoint_data = pd.read_csv(os.path.join(result_dir, dir_prefix, "checkpoints.txt"),
                           delimiter=" ", index_col=False)
    checkpoint_data["startTimeFromZero"] = checkpoint_data["startTime_ms"] - minTime
    checkpoint_data.sort_values("startTimeFromZero", inplace=True)
    checkpoint_data = checkpoint_data[(checkpoint_data["startTimeFromZero"] >= TIME_BEGIN) & (checkpoint_data["startTimeFromZero"] <= length)]
    return checkpoint_data

def zk1_cpu_df(result_dir, dir_prefix):
    resource_data = pd.read_csv(os.path.join(result_dir, dir_prefix, "cpu-zk1.txt"),
                               delimiter=" ", index_col=False)
    minTime = resource_data["timestamp"].min()
    resource_data["timeFromZero"] = resource_data["timestamp"] - minTime
    resource_data.sort_values("timeFromZero", inplace=True)
    return resource_data

def resource_df(result_dir, dir_prefix, resource, host, minTime, length):
    resource_data = pd.read_csv(os.path.join(result_dir, dir_prefix, resource + "-" + host + ".txt"),
                               delimiter=" ", index_col=False)
    resource_data["timeFromZero"] = resource_data["timestamp"] - minTime
    resource_data.sort_values("timeFromZero", inplace=True)
    resource_data = resource_data[(resource_data["timeFromZero"] >= TIME_BEGIN) & (resource_data["timeFromZero"] <= length)]
    return resource_data


# In[38]:


def export_plots(path, fig_name, fig):
    if not os.path.exists(path):
        os.mkdir(path)
    fig.write_image(os.path.join(path, fig_name+".png"))

def latency_plot(latency_data, marker_mode=False):
    if marker_mode:
        return go.Scatter(
        x = latency_data.time,
        y = latency_data.latency, 
        mode="markers", marker=dict(size=2),
        name = "latency")
    else:
        return go.Scatter(
            x = latency_data.time,
            y = latency_data.latency, 
            name = "latency")

def throughput_plot(result_dir, dir_prefix, minTime, length, tm_list):
    all_tm_throughput = pd.concat([
        throughput_df(result_dir, dir_prefix, tm+".txt", minTime, length) 
        for tm in tm_list
        if os.path.exists(os.path.join(result_dir, dir_prefix, tm+".txt"))
    ])[["startTimeFromZero", "elements/second/core"]]
    all_tm_throughput.sort_values("startTimeFromZero")
    groups = pd.cut(all_tm_throughput["startTimeFromZero"], np.arange(all_tm_throughput["startTimeFromZero"].min(), all_tm_throughput["startTimeFromZero"].max(), 1000))
    res = all_tm_throughput.groupby(groups).sum()
    res = pd.DataFrame({"elements/second": res["elements/second/core"].to_numpy(), "startTimeFromZero": res.index.categories.left})
#     res = res[res["elements/second"] > 0]
    
    return go.Scatter(
        x = res["startTimeFromZero"],
        y = res["elements/second"], 
        line_width=1,
#         mode="markers", marker=dict(symbol="triangle-up", size=3),
        name = "Throughput")

def output_throughput_plot(latency_data):
    groups = pd.cut(latency_data["time"], np.arange(latency_data["time"].min(), latency_data["time"].max(), 1000))
    res = latency_data.groupby(groups).count()
    res = pd.DataFrame({"elements/second": res["count"].to_numpy(), "startTimeFromZero": res.index.categories.left})
    return go.Scatter(
        x = res["startTimeFromZero"],
        y = res["elements/second"], 
        line_width=1,
#         mode="markers", marker=dict(symbol="triangle-up", size=3),
        name = "Output Throughput")

# def throughput_plot(result_dir, dir_prefix, minTime, tm_list):
#     all_tm_throughput = pd.concat([throughput_df(result_dir, dir_prefix, tm+".txt", minTime) for tm in tm_list])
#     all_tm_throughput.sort_values("startTimeFromZero")
#     return go.Histogram(x=all_tm_throughput["startTimeFromZero"],
#                         y=all_tm_throughput["elements/second/core"],
#                         name="throughput",
#                         xbins=dict(start=0, end=all_tm_throughput["startTimeFromZero"].max(), size=1000),
#                         histfunc="sum",
#                         opacity=1,
#                        )

# throughput_colors = [ '#AB63FA', '#FFA15A', '#19D3F3', '#FF6692', '#B6E880', '#FF97FF', '#FECB52']
# def throughput_plot(tm, throughput_data, color_idx):
#     return go.Scatter(
#         x = throughput_data["startTimeFromZero"],
#         y = throughput_data["elements/second/core"], 
#         mode="markers", marker=dict(symbol="triangle-up", color=throughput_colors[color_idx]),
#         name = tm + "_throughput")
       
def cpu_usage_plot(cpu, column, name):
    return go.Scatter(
            x = cpu.timeFromZero,
            y = cpu[column],
            line_width=1,
            name = name
        )

def memory_usage_plot(memory):
    return go.Scatter(
            x = memory.timeFromZero,
            y = memory["used_percent"],
            line=dict(dash = 'dot'),
            name = "Memory Used"
        )

# def memory_cache_plot(memory):
#     return go.Scatter(
#             x = memory.timeFromZero,
#             y = memory["cache_percent"],
#             line=dict(dash = 'dash'),
#             name = "Memory Cached"
#         )


def network_read_plot(network):
    return go.Scatter(
            x = network.index,
            y = network["recv_bytes"],
            line=dict(width=1, color="cyan"),
            name = "Network",
            legendgroup = "Network",
            showlegend = True,
        )
def network_write_plot(network):    
    return go.Scatter(
            x = network.index,
            y = network["sent_bytes"],
            line=dict(width=1, color="cyan"),
            name = "Network",
            legendgroup = "Network",
            showlegend = False,
        )

def disk_read_plot(disk):    
    return go.Scatter(
            x = disk.index,
            y = disk["total_read_bytes_per_s"],
            line=dict(width=1, dash = 'dashdot', color="orange"),
            name = "Disk",
            legendgroup = "Disk",
            showlegend = True,
        )
def disk_write_plot(disk):
    return go.Scatter(
            x = disk.index,
            y = disk["total_write_bytes_per_s"],
            line=dict(width=1, dash = 'dashdot', color="orange"),
            name = "Disk",
            legendgroup = "Disk",
            showlegend = False,
        )

def recovered_failure_plot(recovered_failure, showlegend):
    res = []
    for i, (start, end) in enumerate(zip(recovered_failure["failedTimeFromZero"], recovered_failure["recoveredTimeFromZero"])):
        res.append(go.Scatter(x=[start, end],
                   y=[0, 0],
                   name="Recovered Failure",
                   legendgroup="Recovered Failure",
                    marker=dict(color="red", symbol="x-thin",size=10, line=dict(color="red", width=1)),          
#                    marker=dict(symbol="x",size=7,color="rgba(255, 0, 0, 1)",line=dict(color="red", width=2)),
               showlegend= i == 0 if showlegend else False))        
    return res

def recovered_failure_plot(recovered_failure, max_y, fig):
    for i, (start, cost) in enumerate(zip(recovered_failure["failedTimeFromZero"], recovered_failure["RecoveryLength_ms"])):
        fig.add_vrect(
            x0=start, 
            x1=start + cost,
            fillcolor="grey", 
            opacity=0.5,
            layer="below", 
            line_color="black",
            line_width=1,
            row="all", col="all"
        )
    
#         res.append(go.Scatter(x=[start, start + cost],
#                    y=[0, 0],
#                    name="Recovered Failure",
#                    legendgroup="Recovered Failure",
#                     marker=dict(color="red", symbol="x-thin",size=10, line=dict(color="red", width=1)),          
# #                    marker=dict(symbol="x",size=7,color="rgba(255, 0, 0, 1)",line=dict(color="red", width=2)),
#                showlegend= i == 0 if showlegend else False))    
    

def unrecovered_failure_plot(unrecovered_failure, showlegend):
    failure_y = np.empty(len(unrecovered_failure))
    failure_y.fill(0)
    return go.Scatter(
            x = unrecovered_failure["failedTimeFromZero"],
            y = failure_y,
            mode="markers", 
            marker=dict(symbol="x",size=7,color="rgba(255, 0, 0, 0)", line=dict(color="red", width=1)),
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


# In[42]:


TIME_BEGIN, TIME_END = 0, 4000000
tm_list = ["flink" + str(num) for num in range(2, 18)]
def latency_throughput_plot(result_dir, dir_prefix, export=False, marker_mode=False, include_title=True):
    zk_cpu = zk1_cpu_df(result_dir, dir_prefix)
    zk_cpu = zk_cpu[(TIME_BEGIN <= zk_cpu["timeFromZero"]) & (zk_cpu["timeFromZero"] <= TIME_END)]
    minTime = zk_cpu["timestamp"].min()
    if minTime is np.nan:
        minTime = 0
        
    latency_data = latency_df(result_dir, dir_prefix, minTime)
    length = min(latency_data["time"].max(), TIME_END)
    latency_data = latency_data[(TIME_BEGIN <= latency_data["time"]) & (latency_data["time"] <= length)]
    
    print("time length: ", latency_data["time"].max(), " || ", "num windows: ", len(latency_data))

    failure_data = failure_df(result_dir, dir_prefix, minTime, length)
    checkpoint_data = checkpoint_df(result_dir, dir_prefix, minTime, length)
    
    fig = make_subplots(
        rows=2, cols=1,
        subplot_titles=("Latency Over Time", "Throughput Over Time"),
        specs=[[{"secondary_y": False}], 
               [{"secondary_y": True}]])
    #row1
    fig.update_xaxes(title_text="Time (ms)", title_font = {"size": 15}, range=(max(0, TIME_BEGIN), min(length, TIME_END)), row=1, col=1)
    fig.update_yaxes(title_text="Latency (ms)", title_font = {"size": 15}, row=1, col=1)
    #row2
    fig.update_xaxes(title_text="Time (ms)", title_font = {"size": 15}, range=(max(0, TIME_BEGIN), min(length, TIME_END)), row=2, col=1)
    fig.update_yaxes(title_text="Input Throughput (elements/s)", title_font = {"size": 15}, row=2, col=1, secondary_y=False)
    fig.update_yaxes(title_text="Output Throughput (elements/s)", title_font = {"size": 15}, row=2, col=1, secondary_y=True)
    
    #legend
    fig.update_layout(legend=dict(
        orientation="h",
        yanchor="top", xanchor="center",
        x=0.5, y=-0.12
    ))
    
    if include_title:
        fig.update_layout(title=dir_prefix)
    else:
        fig.update_layout(margin={'t': 20, 'b': 0})
         
    #general layout
    fig.update_layout(
        font=dict(
            family="Courier New, monospace", # 所有标题文字的字体
            size=16,                         # 所有标题文字的大小
            color="RebeccaPurple"            # 所有标题的颜色      
        ),
        height=600
    )
    
    #latency
    fig.add_trace(latency_plot(latency_data, marker_mode=marker_mode), row=1, col=1)
       
    # throughputs
    fig.add_trace(throughput_plot(result_dir, dir_prefix, minTime, length, tm_list), row=2, col=1)
    fig.add_trace(output_throughput_plot(latency_data), row=2, col=1, secondary_y=True)
#     throughput_data_list = [(tm, throughput_df(result_dir, dir_prefix, tm + ".txt", minTime))
#                             for tm in tm_list]
#     for idx, (tm, throughput_data) in enumerate(throughput_data_list):
#         fig.add_trace(throughput_plot(tm, throughput_data, idx), secondary_y=True)

    # failures
    recovered_failure = failure_data[(failure_data["loadCheckpointCompleteTime"] > 0) & (failure_data["RecoveryStartTime"] > 0)]
    ones = [1] * len(recovered_failure)
#     fig.add_traces(recovered_failure_plot(recovered_failure, True), rows=ones, cols=ones) 
#     fig.add_traces(recovered_failure_plot(recovered_failure, False), rows=[2] * len(recovered_failure), cols=ones)
    recovered_failure_plot(recovered_failure, latency_data["latency"].max(), fig)

    unrecovered_failure = failure_data[(failure_data["loadCheckpointCompleteTime"] <= 0) | (failure_data["RecoveryStartTime"] <= 0)]
    fig.add_trace(unrecovered_failure_plot(unrecovered_failure, True), row=1, col=1)
    fig.add_trace(unrecovered_failure_plot(unrecovered_failure, False), row=2, col=1)

    # checkpoints
    successful_checkpoints = checkpoint_data[checkpoint_data["size_bytes"] > 0]
    ones = [1] * len(successful_checkpoints)
    fig.add_traces(successful_checkpoints_plot(successful_checkpoints, True), rows=ones, cols=ones)
    fig.add_traces(successful_checkpoints_plot(successful_checkpoints, False), rows=[2] * len(successful_checkpoints), cols=ones)

    failed_checkpoints = checkpoint_data[checkpoint_data["size_bytes"] == 0]
    ones = [1] * len(failed_checkpoints)
    fig.add_traces(failed_checkpoints_plot(failed_checkpoints, True), rows=ones, cols=ones)
    fig.add_traces(failed_checkpoints_plot(failed_checkpoints, False), rows=[2] * len(failed_checkpoints), cols=ones)
    
    if (export is True):
        export_plots(FIGURE_DIR, dir_prefix + "-latency", fig)
        
    return fig

SAMPLE_RATE_S = 2
def resource_plot(result_dir, dir_prefix, host, export=False, include_title=True):
    zk_cpu = zk1_cpu_df(result_dir, dir_prefix)
    zk_cpu = zk_cpu[(TIME_BEGIN <= zk_cpu["timeFromZero"]) & (zk_cpu["timeFromZero"] <= TIME_END)]
    minTime = zk_cpu["timestamp"].min()
    if minTime is np.nan:
        minTime = 0
        
    latency_data = latency_df(result_dir, dir_prefix, minTime)
    length = min(latency_data["time"].max(), TIME_END)
    latency_data = latency_data[(TIME_BEGIN <= latency_data["time"]) & (latency_data["time"] <= length)]
    
    failure_data = failure_df(result_dir, dir_prefix, minTime, length)
    checkpoint_data = checkpoint_df(result_dir, dir_prefix, minTime, length)
    
    # Create figure with secondary y-axis
    #fig = make_subplots(specs=[[{"secondary_y": True}]])
    fig = make_subplots(
        rows=3, cols=1,
        subplot_titles=("CPU/Memory Usage Over Time", "Dist/Network Reads Over Time",  "Dist/Network Writes Over Time"))
    y_title_size = 15
    #row1
    fig.update_yaxes(title_text="Usage(%)", title_font = {"size": y_title_size}, row=1, col=1)
    fig.update_xaxes(title_text="timestamp (ms)", title_font = {"size": 15}, range=(max(0, TIME_BEGIN), min(length, TIME_END)), row=1, col=1)
    #row2
    fig.update_yaxes(title_text="Read Rate (Bytes/s)", title_font = {"size": y_title_size}, row=2, col=1)
    fig.update_xaxes(title_text="timestamp (ms)", title_font = {"size": 15}, range=(max(0, TIME_BEGIN), min(length, TIME_END)), row=2, col=1)
    #row3
    fig.update_yaxes(title_text="Write Rate (Bytes/s)", title_font = {"size": y_title_size}, row=3, col=1)
    fig.update_xaxes(title_text="timestamp (ms)", title_font = {"size": 15}, range=(max(0, TIME_BEGIN), min(length, TIME_END)), row=3, col=1)

    #general layout
    if include_title:
        fig.update_layout(title=dir_prefix + "-" + host)
    else:
        fig.update_layout(margin={'t': 20, 'b': 0})
        
    fig.update_layout(
        font=dict(
            family="Courier New, monospace", # 所有标题文字的字体
            size=16,                         # 所有标题文字的大小
            color="RebeccaPurple"            # 所有标题的颜色      
        ),
        height=800
    )
    #legend
    fig.update_layout(legend=dict(
        orientation="h",
        yanchor="top", xanchor="center",
        x=0.5, y=-0.1
    ))

# timestamp r b swpd free buff cache si so bi bo in cs us sy id wa st    
#     cpu_mem = resource_df(result_dir, dir_prefix, "cpu-mem", host, minTime)
    #cpu
    cpu = resource_df(result_dir, dir_prefix, "cpu", host, minTime, length)
    cpu["used"] = 100 - cpu["idle"]
#     cpu_mem["cpu_used"] = 100 - cpu_mem["id"]
    fig.add_trace(cpu_usage_plot(cpu, "used", "CPU Total Usage"), row=1, col=1)
    fig.add_trace(cpu_usage_plot(cpu, "user_space", "CPU User Space Usage"), row=1, col=1)
    fig.add_trace(cpu_usage_plot(cpu, "system", "CPU System Usage"), row=1, col=1)

    #memory
    heap_file = "heap-" + host + ".txt"
    if host in tm_list and os.path.exists(os.path.join(result_dir,dir_prefix,heap_file)):
        memory = resource_df(result_dir, dir_prefix, "heap", host, minTime, length)
        memory["used_percent"] = (memory["used"] / memory["max"]) * 100
    else:  
        print(heap_file, " not found. Will use 'top' recording instead")
        memory = resource_df(result_dir, dir_prefix, "memory", host, minTime, length)
        memory["used_percent"] = (memory["used"] / memory["total"]) * 100
#         memory["cache_percent"] = (memory["buff/cache"] / memory["total"]) * 100
#     memory = resource_df(result_dir, dir_prefix, "cpu-mem", host, minTime)
#     memory["used_percent"] = (memory["used"] / memory["total"]) * 100
#     mem_total = 16266168
#     cpu_mem["mem_used_percent"] = 100 - (cpu_mem["free"] / mem_total * 100)
#     cpu_mem["mem_cache_percent"] = cpu_mem["cache"] / mem_total * 100
    fig.add_trace(memory_usage_plot(memory), row=1, col=1)
#     fig.add_trace(memory_cache_plot(memory), row=1, col=1)
    #network
    network = resource_df(result_dir, dir_prefix, "network", host, minTime, length)
    network = network.drop(columns=["interface", "timestamp"])
    # calculate diff between consecutive rows except for timestamp
    network["index"] = network["timeFromZero"]
    network = network.set_index("index").diff() 
    network = network[network["timeFromZero"] > 0]
    # divide by sample rate
    network = network.div(network["timeFromZero"] / 1000, axis=0)

    fig.add_trace(network_read_plot(network), row=2, col=1)
    fig.add_trace(network_write_plot(network), row=3, col=1)    
    
    #disk
    # timestamp disk read_total read_merged read_sectors read_ms write_total write_merged write_sectors write_ms io_cur io_sec
    disk = resource_df(result_dir, dir_prefix, "disk", host, minTime, length)
    disk = disk.drop(columns=["disk", "timestamp"])
    # calculate diff between consecutive rows except for timestamp
    disk["index"] = disk["timeFromZero"]
    disk = disk.set_index("index").diff() 
    disk = disk[disk["timeFromZero"] > 0]
    # divide by sample rate
    disk = disk.div(disk["timeFromZero"] / 1000, axis=0)

    # Sector is 512 bytes
    disk["total_read_bytes_per_s"] = disk["read_sectors"] * 512
    disk["total_write_bytes_per_s"] = disk["write_sectors"] * 512
    fig.add_trace(disk_read_plot(disk), row=2, col=1)
    fig.add_trace(disk_write_plot(disk), row=3, col=1)

    # failures
    recovered_failure = failure_data[(failure_data["loadCheckpointCompleteTime"] > 0) & (failure_data["RecoveryStartTime"] > 0)]
#     recovered_failure = recovered_failure[recovered_failure["checkpointId"] % 3 == 0]
    ones = [1] * len(recovered_failure)
#     fig.add_traces(recovered_failure_plot(recovered_failure, True), rows=ones, cols=ones)
#     fig.add_traces(recovered_failure_plot(recovered_failure, False), rows=[2] * len(recovered_failure), cols=ones)
#     fig.add_traces(recovered_failure_plot(recovered_failure, False), rows=[3] * len(recovered_failure), cols=ones)
    recovered_failure_plot(recovered_failure, latency_data["latency"].max(), fig)

    unrecovered_failure = failure_data[(failure_data["loadCheckpointCompleteTime"] <= 0) | (failure_data["RecoveryStartTime"] <= 0)]
    fig.add_trace(unrecovered_failure_plot(unrecovered_failure, True), row=1, col=1)
    fig.add_trace(unrecovered_failure_plot(unrecovered_failure, False), row=2, col=1)
    fig.add_trace(unrecovered_failure_plot(unrecovered_failure, False), row=3, col=1)

    # checkpoints
    successful_checkpoints = checkpoint_data[checkpoint_data["size_bytes"] > 0]
    ones = [1] * len(successful_checkpoints)
    fig.add_traces(successful_checkpoints_plot(successful_checkpoints, True), rows=ones, cols=ones)
    fig.add_traces(successful_checkpoints_plot(successful_checkpoints, False), rows=[2] * len(successful_checkpoints), cols=ones)
    fig.add_traces(successful_checkpoints_plot(successful_checkpoints, False), rows=[3] * len(successful_checkpoints), cols=ones)

    failed_checkpoints = checkpoint_data[checkpoint_data["size_bytes"] == 0]
    ones = [1] * len(failed_checkpoints)
    fig.add_traces(failed_checkpoints_plot(failed_checkpoints, True), rows=ones, cols=ones)
    fig.add_traces(failed_checkpoints_plot(failed_checkpoints, False), rows=[2] * len(failed_checkpoints), cols=ones)
    fig.add_traces(failed_checkpoints_plot(failed_checkpoints, False), rows=[3] * len(failed_checkpoints), cols=ones)
    
    if (export is True):
        export_plots(FIGURE_DIR, dir_prefix + "-" + host, fig)
        
    return fig
    


# In[43]:


# case study
result_dir="C:\\Users\\joinp\\Downloads\\problematic\\"
result_dir="C:\\Users\\joinp\\Downloads\\results\\"
# result_dir="C:\\Users\\joinp\\Downloads\\tofix\\"
dir_prefix="09-23_15-43-41_load-10000-multi"
TIME_BEGIN, TIME_END = 0, 1000000
# hosts=["flink" + str(num) for num in range(2,18)]
hosts=[]
# hosts = ["flink1"]
export = False
latency_throughput_plot(os.path.abspath(result_dir), dir_prefix, marker_mode=False, export=export).show(scale=2)
for host in hosts:
    resource_plot(result_dir, dir_prefix, host, export=export).show(scale=2)


# In[27]:


# draw all
result_dir="C:\\Users\\joinp\\Downloads\\results\\"
# result_dir="C:\\Users\\joinp\\Downloads\\tofix\\"
# TIME_BEGIN, TIME_END = 40000, 100000
TIME_BEGIN, TIME_END = 0, 10000000

hosts=[]
# hosts=["flink2","hadoop2","kafka2"]
# hosts=["hadoop1","hadoop2","hadoop3","hadoop4","flink1","flink2","flink3","flink4","flink5","kafka1","kafka2","redis1","zk1"]
export = False
for dir_prefix in os.listdir(result_dir):
    if os.path.isdir(result_dir+dir_prefix):
        print(dir_prefix)
        latency_throughput_plot(os.path.abspath(result_dir), dir_prefix, export=export, marker_mode=False).show(scale=2)
        for host in hosts:
            resource_plot(result_dir, dir_prefix, host, export=export).show(scale=2)


# In[6]:


# # average latency under no failures
# result_dir="C:\\Users\\joinp\\Downloads\\results\\"
# TIME_BEGIN, TIME_END = 0, 10000000

# hosts=[]
# # hosts=["flink2","hadoop2","kafka2"]
# # hosts=["hadoop1","hadoop2","hadoop3","hadoop4","flink1","flink2","flink3","flink4","flink5","kafka1","kafka2","redis1","zk1"]
# export = False
# for dir_prefix in os.listdir(result_dir):
#     if os.path.isdir(result_dir+dir_prefix):
#         print(dir_prefix)
#         zk_cpu = zk1_cpu_df(result_dir, dir_prefix)
#         zk_cpu = zk_cpu[(TIME_BEGIN <= zk_cpu["timeFromZero"]) & (zk_cpu["timeFromZero"] <= TIME_END)]
#         minTime = zk_cpu["timestamp"].min()
#         if minTime is np.nan:
#             minTime = 0

#         latency_data = latency_df(result_dir, dir_prefix, minTime)
#         length = min(latency_data["time"].max(), TIME_END)
#         latency_data = latency_data[(TIME_BEGIN <= latency_data["time"]) & (latency_data["time"] <= length)]
#         display(latency_data[latency_data["latency"] < 1000].mean()["latency"].round())


# In[7]:


# case study
# result_dir="C:\\Users\\joinp\\Downloads\\problematic\\"
# result_dir="C:\\Users\\joinp\\Downloads\\results\\"
# # result_dir="C:\\Users\\joinp\\Downloads\\tofix\\"
# dir_prefix="09-22_21-21-15_load-80000-multi"
# TIME_BEGIN, TIME_END = 0, 4000000
# hosts=["flink" + str(num) for num in range(2,18)]
# # hosts=[]
# # hosts = ["flink1"]
# export = False
# latency_throughput_plot(os.path.abspath(result_dir), dir_prefix, marker_mode=False, export=export).show(scale=2)
# for host in hosts:
#     resource_plot(result_dir, dir_prefix, host, export=export).show(scale=2)


# In[8]:


# result_dir="C:\\Users\\joinp\\Downloads\\problematic\\"
# result_dir="C:\\Users\\joinp\\Downloads\\result-9\\"
# dir_prefix="09-22_04-24-38_load-30000-multi"
# dir_prefix= "09-20_15-33-13_load-15000-multi"
# cpu = zk1_cpu_df(result_dir, dir_prefix)
# cpu = cpu[(TIME_BEGIN <= cpu["timeFromZero"]) & (cpu["timeFromZero"] <= TIME_END)]
# minTime = cpu["timestamp"].min()
# if minTime is np.nan:
#     minTime = 0


# In[9]:


# result_dir = "C:\\Users\\joinp\\Downloads\\results\\"
# dirs = os.listdir(result_dir)
# [path for path in dirs if os.path.isdir(result_dir+path)]


# In[10]:


SLOT_PER_NODE = 2
def recover_cost_df(result_dir, dir_prefix, minTime, length, parallelism):
    checkpoint_data = checkpoint_df(result_dir, dir_prefix, minTime, length)
    checkpoint_data = checkpoint_data[checkpoint_data["size_bytes"] > 0]

    failure_data = failure_df(result_dir, dir_prefix, minTime, length)
    failure_data = failure_data[(failure_data["checkpointId"] > 0) & 
                                (failure_data["RecoveryStartTime"] > 0) & 
                                (failure_data["loadCheckpointCompleteTime"] > 0)]
    res = pd.merge(left=failure_data, right=checkpoint_data[["id", "size_bytes", "timeCost_ms"]], how='left',
                   left_on='checkpointId', right_on='id')
    res["size_MB/Node"] = res["size_bytes"] / 1000000 / parallelism * SLOT_PER_NODE 
    res["checkpoint speed per node (MB/Sec)"] = res["size_MB/Node"] / res["timeCost_ms"] * 1000
    res["recovery speed per node (MB/Sec)"] = res["size_MB/Node"] / res["RecoveryLength_ms"] * 1000
    res["parallelism"] = parallelism
    return res.rename(columns={
        'timeCost_ms': 'checkpoint cost (ms)', 
        'size_MB/Node': 'checkpoint size per node (MB)',
        'RecoveryLength_ms': 'recovery cost (ms)',
    })


def checkpoint_cost_df(result_dir, dir_prefix, minTime, length, parallelism, failure_data=None):
    checkpoint_data = checkpoint_df(result_dir, dir_prefix, minTime, length)
    checkpoint_data = checkpoint_data[checkpoint_data["size_bytes"] > 0]
    
    # drop checkpoints during failure (they are too long than normal checkpoints)
    if failure_data is not None:
        checkpoint_data["endTimeFromZero"] = checkpoint_data["startTimeFromZero"] + checkpoint_data["timeCost_ms"]
        chechpoint_during_failure = checkpoint_data.apply(
            lambda row: np.any(
                ((failure_data["failedTimeFromZero"] <= row["startTimeFromZero"]) & (failure_data["recoveredTimeFromZero"] >= row["startTimeFromZero"]))
            ),
            axis=1)
        checkpoint_data = checkpoint_data[~chechpoint_during_failure]
    
    checkpoint_data["parallelism"] = parallelism
    checkpoint_data["size per node (MB)"] = checkpoint_data["size_bytes"] / 1024 / 1024 / parallelism * SLOT_PER_NODE
    checkpoint_data["time cost (s)"] = checkpoint_data["timeCost_ms"] / 1000
    checkpoint_data["speed per node (MB/s)"] = checkpoint_data["size per node (MB)"] * 1000 / checkpoint_data["timeCost_ms"]
    return checkpoint_data

def grep_parallelism(file):
    # FLINK_PARALLELISM=$FLINK_PARALLELISM

    with open(file, 'r') as file:
        for line in file:
            if re.search("# FLINK_PARALLELISM=", line):
                return int(line[20:])
            
def grep_config(file, toSearch):
    #multilevel.enable: false
    with open(file, 'r') as file:
        for line in file:
            if re.search(toSearch, line):
                return line.split(':')[1].strip().strip('"')

# grep_config("C:\\Users\\joinp\\Downloads\\results\\09-21_00-22-26_load-70000-single\\conf-copy.yaml", "multilevel.pattern")


# In[11]:


def add_checkpoint_level(config_file, df, id_key):
    multilevel_enabled = grep_config(config_file, "multilevel.enable:")
    if multilevel_enabled == "true":
        multilevel_pattern = grep_config(config_file, "multilevel.pattern:").split(',')
        df["level"] = df.apply(
            lambda row : int(multilevel_pattern[int(row[id_key] - 1) % len(multilevel_pattern)]),
            axis = 1)
    else:
        df["level"] = 2

def checkpoint_recovery_cost(result_dir):
    checkpoint_cost = None
    recovery_cost = None
    for dir_prefix in os.listdir(result_dir):    
        if os.path.isdir(result_dir+dir_prefix):
            cpu_data = zk1_cpu_df(result_dir, dir_prefix)
            minTime = cpu_data["timestamp"].min()
            length = cpu_data["timeFromZero"].max()


            config_file = os.path.join(result_dir, dir_prefix, "conf-copy.yaml")
            parallelism = grep_parallelism(config_file)
            
            failure_data = recover_cost_df(result_dir, dir_prefix, minTime, 4000000, parallelism)
            add_checkpoint_level(config_file, failure_data, "checkpointId")
                
            recovery_data = failure_data[[
                "failedTimeFromZero",
                "checkpointId",
                "checkpoint size per node (MB)",
                "checkpoint cost (ms)",
                "recovery cost (ms)",
                "checkpoint speed per node (MB/Sec)",
                "recovery speed per node (MB/Sec)",
                "level",
                "parallelism",
            ]]

            checkpoint_data = checkpoint_cost_df(result_dir, dir_prefix, minTime, length, parallelism, failure_data=failure_data)
            checkpoint_data = checkpoint_data[2:-2] # usually the first and last few checkpoints are small
            checkpoint_data = checkpoint_data[(checkpoint_data["size_bytes"] > 0) & (checkpoint_data["timeCost_ms"] > 0)]
            add_checkpoint_level(config_file, checkpoint_data, "id")
           
            checkpoint_data = checkpoint_data[["id",
                    "size per node (MB)",
                    "time cost (s)",
                    "speed per node (MB/s)",
                    "level",
                    "parallelism",
                   ]]
            
            if checkpoint_cost is None:
                checkpoint_cost = checkpoint_data
            else:
                checkpoint_cost = checkpoint_cost.append(checkpoint_data)

            if recovery_cost is None:
                recovery_cost = recovery_data
            else:
                recovery_cost = recovery_cost.append(recovery_data)

    return checkpoint_cost, recovery_cost

def checkpoint_cost_all(result_dir):
    checkpoint_cost = None
    for dir_prefix in os.listdir(result_dir):    
        if os.path.isdir(result_dir+dir_prefix):
            latency_data = latency_df(result_dir, dir_prefix)
            minTime = latency_data["currTime"].min()
            length = latency_data["time"].max()

            config_file = os.path.join(result_dir, dir_prefix, "conf-copy.yaml")
            parallelism = grep_parallelism(os.path.join(result_dir, dir_prefix, "conf-copy.yaml"))

def mean_and_error_bar(df, mean_df):
    max_val = df.max()
    min_val = df.min()
    
    print(max_val, min_val)
    
    error_up = max_val - mean_df
    error_down = mean_df - min_val
    return error_up, error_down

def avg_cost_by_parallelism(cost_df):
    LOW_LEVEL_SIGN = 1
    HIGH_LEVEL_SIGN = 2

    low_level_cost = cost_df[cost_df["level"] == LOW_LEVEL_SIGN]
    high_level_cost = cost_df[cost_df["level"] == HIGH_LEVEL_SIGN]

    group_cost_low = low_level_cost.groupby(low_level_cost["parallelism"])
    group_cost_high = high_level_cost.groupby(high_level_cost["parallelism"])
    #return mean and std-err
    low_mean = group_cost_low.mean()
    low_mean["count"] = group_cost_low.count()["level"]
    high_mean = group_cost_high.mean()
    high_mean["count"] = group_cost_high.count()["level"]
    return low_mean, group_cost_low.sem(), high_mean, group_cost_high.sem()

# checkpoint_cost, recovery_cost = checkpoint_recovery_cost(result_dir)
# display(recovery_cost)
# display(checkpoint_cost)


# In[12]:


# LOW_LEVEL_SIGN = 1
# HIGH_LEVEL_SIGN = 2
# checkpoint_cost, recovery_cost = checkpoint_recovery_cost(result_dir)
# cost_df = checkpoint_cost

# mean_cost_low, std_err_low, mean_cost_high, std_err_high = avg_cost_by_parallelism(cost_df)
# display(mean_cost_low)
# display(std_err_low)
# display(mean_cost_high)
# std_err_high


# In[13]:


# result_dir="C:\\Users\\joinp\\Downloads\\results\\"
# dir_prefix="09-21_16-12-38_load-80000-multi"
# cpu_data = zk1_cpu_df(result_dir, dir_prefix)
# minTime = cpu_data["timestamp"].min()
# length = cpu_data["timeFromZero"].max()
# display(minTime, length)

# config_file = os.path.join(result_dir, dir_prefix, "conf-copy.yaml")
# parallelism = grep_parallelism(config_file)

# failure_data = recover_cost_df(result_dir, dir_prefix, minTime, 4000000, parallelism)
# add_checkpoint_level(config_file, failure_data, "checkpointId")

# recovery_data = failure_data[[
#     "failedTimeFromZero",
#     "checkpointId",
#     "checkpoint size per node (MB)",
#     "checkpoint cost (ms)",
#     "recovery cost (ms)",
#     "checkpoint speed per node (MB/Sec)",
#     "recovery speed per node (MB/Sec)",
#     "level",
#     "parallelism",
# ]]

# checkpoint_data = checkpoint_cost_df(result_dir, dir_prefix, minTime, length, parallelism, failure_data=failure_data)
# checkpoint_data["endTimeFromZero"] = checkpoint_data["startTimeFromZero"] + checkpoint_data["timeCost_ms"]

# chechpoint_during_failure = checkpoint_data.apply(
#     lambda row: np.any(
#         ((failure_data["failedTimeFromZero"] <= row["startTimeFromZero"]) & (failure_data["recoveredTimeFromZero"] >= row["startTimeFromZero"])) | \
#         ((failure_data["failedTimeFromZero"] <= row["endTimeFromZero"]) & (failure_data["recoveredTimeFromZero"] >= row["endTimeFromZero"]))
#     ),
#     axis=1)
# checkpoint_data[~chechpoint_during_failure]


# In[14]:


result_dir="C:\\Users\\joinp\\Downloads\\results\\"
# result_dir="C:\\Users\\joinp\\Downloads\\2-slot-300ms-buffer\\"

x_start = 4
interval = 4

checkpoint_cost, recovery_cost = checkpoint_recovery_cost(result_dir)

keys_recovery = "checkpointId,checkpoint size per node (MB),checkpoint cost (ms),recovery cost (ms),checkpoint speed per node (MB/Sec),recovery speed per node (MB/Sec),level".split(',')
recovery_low_mean, recovery_low_err, recovery_high_mean, recovery_high_err = avg_cost_by_parallelism(recovery_cost)

keys_checkpoint = "id,size_bytes,level".split(',')
checkpoint_low_mean, checkpoint_low_err, checkpoint_high_mean, checkpoint_high_err = avg_cost_by_parallelism(checkpoint_cost)

def mean_cost_plot(df, df_err, key, legend_name):
    return go.Bar(
#     return go.Scatter(
        x=df.index,
        y=df[key],
        name=legend_name,
        error_y=dict(
            type='data', # use exact value instead of percentage
            array=df_err[key]),
    )

x_axes_layout = dict(
        tickmode = 'linear',
        tick0 = x_start,
        dtick = interval,
        title = "Parallelism"
    )

cr_cost_fig= go.Figure()
# checkpoint speed
cr_cost_fig.add_trace(mean_cost_plot(checkpoint_low_mean, checkpoint_low_err, "speed per node (MB/s)", "Low Level Checkpoint Speed"))
cr_cost_fig.add_trace(mean_cost_plot(checkpoint_high_mean, checkpoint_high_err, "speed per node (MB/s)", "High Level Checkpoint Speed"))
# recovery speed
cr_cost_fig.add_trace(mean_cost_plot(recovery_low_mean, recovery_low_err, "recovery speed per node (MB/Sec)", "Low Level Recovery Speed"))
cr_cost_fig.add_trace(mean_cost_plot(recovery_high_mean, recovery_high_err, "recovery speed per node (MB/Sec)", "High Level Recovery Speed"))

cr_cost_fig.update_layout(xaxis = x_axes_layout, margin=dict(t=0))
cr_cost_fig.update_yaxes(title_text="Speed per node (MB/Sec)", title_font = {"size": 15})

# checkpoint_cost_fig = go.Figure()
# checkpoint_cost_fig.add_trace(mean_cost_plot(checkpoint_low_mean, checkpoint_low_err, "speed per node (MB/s)", "Low-level (actual)"))
# checkpoint_cost_fig.add_trace(mean_cost_plot(checkpoint_high_mean, checkpoint_high_err, "speed per node (MB/s)", "High-level (actual)"))
# checkpoint_cost_fig.update_layout(xaxis = x_axes_layout, margin=dict(t=0))
# checkpoint_cost_fig.update_yaxes(title_text="Checkpoint speed per node (MB/Sec)", title_font = {"size": 15})

# rate_fig= go.Figure()
# rate = checkpoint_low / checkpoint_high
# rate_fig.add_trace(go.Scatter(
#     x=rate.index,
#     y=rate["speed per node (MB/s)"],
#     name="low / high"
# ))
# rate_fig.update_layout(xaxis = x_axes_layout)
# rate_fig.update_yaxes(title_text="Checkpoint speed ratio (low/high)", title_font = {"size": 15})


def ideal_line(x, y, legend_name):
    return go.Scatter(
        x = x,
        y = y,
        name=legend_name
    )

x = np.arange(x_start, 36, interval)

# 1Gbits = 125MB/s, 2 hadoop data node, so 250 bandwidth shared by x subtasks
# highest_net_speed = 250
highest_disk_speed = 55
# ideal_speed_high = highest_net_speed / x
ideal_speed_high = highest_disk_speed * 4 / x
ideal_speed_low = [highest_disk_speed] * len(x)

cr_cost_fig.add_trace(ideal_line(x, ideal_speed_low, "low-level (ideal trend)"))
cr_cost_fig.add_trace(ideal_line(x, ideal_speed_high, "high-level (ideal trend)"))    

# highest_checkpoint_speed = 60
# ideal_checkpoint_speed_high = highest_checkpoint_speed * (0.5 ** np.arange(0, 8))
# ideal_checkpoint_speed_low = np.zeros(8)
# ideal_checkpoint_speed_low.fill(highest_checkpoint_speed)

# checkpoint_cost_fig.add_trace(ideal_line(x, ideal_checkpoint_speed_low, "Low-level (ideal trend)"))
# checkpoint_cost_fig.add_trace(ideal_line(x, ideal_checkpoint_speed_high, "High-level (ideal trend)"))

highest_checkpoint_speed


# In[ ]:



# checkpoint_cost_fig.show()
# recovery_cost_fig.show()
# export_plots(FIGURE_DIR, "mean_checkpoint_cost_fig", checkpoint_fig)
# export_plots(FIGURE_DIR, "mean_recovery_cost_fig", recovery_fig)

cr_cost_fig.show()
# export_plots(FIGURE_DIR, "mean_checkpoint_recovery_cost_fig", cr_cost_fig)

display("recovery_low_mean")
display(recovery_low_mean)
display("recovery_high_mean")
display(recovery_high_mean)
display("checkpoint_low_mean")
display(checkpoint_low_mean)
display("checkpoint_high_mean")
display(checkpoint_high_mean)


# In[ ]:





# In[ ]:


result_dir="C:\\Users\\joinp\\Downloads\\results\\"

for dir_prefix in os.listdir(result_dir):
    if os.path.isdir(result_dir+dir_prefix):
        cpu_data = zk1_cpu_df(result_dir, dir_prefix)
        minTime = cpu_data["timestamp"].min()
        length = cpu_data["timeFromZero"].max()

        parallelism = grep_parallelism(os.path.join(result_dir, dir_prefix, "conf-copy.yaml"))
        
        failure_data = recover_cost_df(result_dir, dir_prefix, minTime, length, parallelism)
        failure_data = failure_data.round(2)
        print(dir_prefix)
        display(failure_data[[
            "failedTimeFromZero",
            "checkpointId",
            "checkpoint size per node (MB)",
            "checkpoint cost (ms)",
            "recovery cost (ms)",
            "checkpoint speed per node (MB/Sec)",
            "recovery speed per node (MB/Sec)",
        ]])


# In[ ]:


# show average cp speed
# result_dir="C:\\Users\\joinp\\Downloads\\2-slot-300ms-buffer\\"

# dirs = os.listdir(result_dir)
# for dir_prefix in dirs:
#     if os.path.isdir(result_dir+dir_prefix):
#         cpu_data = zk1_cpu_df(result_dir, dir_prefix)
#         minTime = cpu_data["timestamp"].min()
#         length = cpu_data["timeFromZero"].max()
        
#         parallelism = grep_parallelism(os.path.join(result_dir, dir_prefix, "conf-copy.yaml"))
        
#         failure_data = recover_cost_df(result_dir, dir_prefix, minTime, length, parallelism)
#         res = checkpoint_cost_df(result_dir, dir_prefix, minTime, length, parallelism, failure_data=failure_data).round(2)
# #         display(res[(res["speed per node (MB/s)"] < 120) & (res["id"] % 2 == 1)])
#         print(dir_prefix)
# #         display(res)
#         display(res[res["id"] % 2==1]["speed per node (MB/s)"].mean())
#         #find experiments with too small states
#         res = res[(res["size per node (MB)"] < 120) & (res["id"] > 2)]
#         if (len(res) > 0):
#             print(dir_prefix)
#             display(res)


# In[ ]:


result_dir="C:\\Users\\joinp\\Downloads\\results\\"
dir_prefix="09-23_15-43-41_load-10000-multi"

zk_cpu = zk1_cpu_df(result_dir, dir_prefix)
zk_cpu = zk_cpu[(TIME_BEGIN <= zk_cpu["timeFromZero"]) & (zk_cpu["timeFromZero"] <= TIME_END)]
minTime = zk_cpu["timestamp"].min()

latency_data = latency_df(result_dir, dir_prefix, minTime)
length = min(latency_data["time"].max(), TIME_END)
latency_data = latency_data[(TIME_BEGIN <= latency_data["time"]) & (latency_data["time"] <= length)]

# selected = latency_data[(latency_data["currTime"] >= 1631167972449) & (latency_data["currTime"] <= 1631168018052)]
# time_len = latency_data["currTime"].max() - latency_data["currTime"].min()
# density_during_recovery = selected["currTime"].count() / (1631168018052 - 1631167972449)
# density_all = latency_data.count() / time_len
# print(density_all)
# print(density_during_recovery)
# density_all / density_during_recovery

failure_data = failure_df(result_dir, dir_prefix, minTime)
failure_data.head()


# In[ ]:





# In[ ]:





# In[ ]:




