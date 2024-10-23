import plotly.graph_objects as go
import urllib.request
import json
import os
import plotly.io as pio
import pandas as pd
from icecream import ic
from builtins import round, min
import random
from collections import defaultdict
import regex as re
import os

def prepare_sankey(df, node_col_list, val_col, val_agg, node_order, show_threshold = 10, node_major_color = "#BF072A", link_major_color = "#D47F8C", minor_color = "#737373"):
    sankey_input = _get_sankey_input(df, node_col_list, val_col, val_agg, show_threshold, node_major_color, link_major_color, minor_color)
    sankey_input['sankey_node_order'] = _get_sankey_node_order(node_order, sankey_input)
    return sankey_input
    
def get_ora_conn():
    # The file contains your Oracle Account
    with open(os.path.join(os.getcwd(), "config.json"), "r") as f:
            config = json.load(f)


    email = config["email"]
    ora_conn = config["ora_conn"] 
    ora_user = config["ora_user"] 
    ora_password = config["ora_password"]
    host = config["host"] 
    port = config["port"] 
    service_name = config["service_name"] 

    # Oracle Session
    dsn_tns = cx_Oracle.makedsn(host, port, service_name= service_name)
    conn = cx_Oracle.connect(user=ora_user, password=ora_password, dsn=dsn_tns)
    cursor = conn.cursor()
    
    return conn

def _random_colors_list(sankey_input, target = "node"):
    if target == "node":
        color_theme = [
            "#E72929",
            "#D24545",
            "#B31312",
            "#C70039",
            "#E06469",
            "#B31312"
        ]
        
        colors_list = []
        
        for i in range(len(sankey_input['display_labels'])):
            colors_list.append(color_theme[random.randint(0, len(color_theme) - 1)])
    else:
        color_theme = [
            "#FCAEAE",
            "#FEA1A1",
            "#EF9F9F",
        ]
        
        colors_list = []
        
        for i in range(len(sankey_input['values'])):
            colors_list.append(color_theme[random.randint(0, len(color_theme) - 1)])
    
    return colors_list
    
def _get_sankey_input(df, node_col_list, val_col, val_agg, show_threshold, node_major_color, link_major_color, minor_color):
    # init
    sankey_input = {
        "display_labels" : {},
        "sources" : [],
        "targets" : [],
        "values" : [],
        "links_percent" : [],
        "nodes_color": {},
        "links_color": [],
        "show_threshold" : show_threshold,
        "color_major_threshold": {"node": 20, "link": 40},
        "stage_nodes_value": defaultdict(lambda: defaultdict(dict))
    }
    
    ######### 1. PERFORM ON FIRST SOURCE-TARGET PAIR
    node_col1 = node_col_list[0]
    node_col2 = node_col_list[1]
    
    df2 = dfd.groupby([node_col1, node_col2], as_index=0).agg(vals = (val_col, val_agg))
    df2[f"{node_col1}_sum"] = df2.groupby(node_col1)['vals'].transform("sum")
    df2[f"{node_col2}_sum"] = df2.groupby(node_col2)['vals'].transform("sum")
    
    total_vals = df2['vals'].sum()
    
    # get percentage on node
    df2[f'%{node_col1}'] = df2[f'{node_col1}_sum'].apply(lambda x: x * 100/total_vals)
    df2[f'%{node_col2}'] = df2[f"{node_col2}_sum"].apply(lambda x: x * 100/total_vals)
    
    # get percentage on link
    df2[f"%{node_col2}/{node_col1}"] = round((df2['vals'] / df2[f"{node_col1}_sum"]) * 100)

    for _, row in df2.iterrows():
        # Add source, target, value
        sankey_input['sources'].append(label_map[row[node_col1]])
        sankey_input['targets'].append(label_map[row[node_col2]])
        sankey_input['values'].append(row['vals'])
        
        # 1. first col
        # Change displayed labels
        _percen_on_node = round(row[f'%{node_col1}'], 2)
        _sum_on_node = row[f"{node_col1}_sum"]
        _percen_on_link = round(row[f"%{node_col2}/{node_col1}"], 2)
        _node_str_name = row[node_col1]
        _node_index = label_map[_node_str_name]
        _split_char = '_'
        
        display_label = "" if _percen_on_node < show_threshold else f"({str(_percen_on_node)}%, {str(_sum_on_node)})"
        sankey_input["display_labels"][_node_index] = f"{_node_str_name.split(_split_char)[-1]} {display_label}"
        sankey_input['nodes_color'][_node_index] = minor_color if _percen_on_node < sankey_input["color_major_threshold"]['node'] else node_major_color
        
        sankey_input["stage_nodes_value"][0][_node_index] = _sum_on_node
        
        # 2. second col
        # Change displayed labels
        _percen_on_node = round(row[f'%{node_col2}'], 2)
        _sum_on_node = row[f"{node_col2}_sum"]
        _node_str_name = row[node_col2]
        _node_index = label_map[_node_str_name]
        _split_char = '_'
        
        display_label = "" if _percen_on_node < show_threshold else f"({str(_percen_on_node)}%, {str(_sum_on_node)})"
        sankey_input["display_labels"][_node_index] = f"{_node_str_name.split(_split_char)[-1]} {display_label}"
        sankey_input['nodes_color'][_node_index] = minor_color if _percen_on_node < sankey_input["color_major_threshold"]['node'] else node_major_color
        
        # Add custom data
        sankey_input["links_percent"].append(_percen_on_link)
        sankey_input['links_color'].append(0 if row[node_col1].split(_split_char)[-1] == "NC" and row[node_col2].split(_split_char)[-1] == "NC" else 1)
    
        sankey_input["stage_nodes_value"][1][_node_index] = _sum_on_node
    
    # counter for stage of display labels
    stage_counter = 2
    
    ######### 2. PERFORM ON OTHERS SOURCE-TARGET PAIR
    if len(node_col_list) <= 2: # if out of index
        return sankey_input
    
    for i, node_col in enumerate(node_col_list[1:]):
        if i == len(node_col_list[1:]) - 1: # if out of index
            return sankey_input
        
        node_col1 = node_col_list[1:][i]
        node_col2 = node_col_list[1:][i + 1]
        
        df2 = dfd.groupby([node_col1, node_col2], as_index=0).agg(vals = (val_col, val_agg))
        df2[f"{node_col1}_sum"] = df2.groupby(node_col1)['vals'].transform("sum")
        df2[f"{node_col2}_sum"] = df2.groupby(node_col2)['vals'].transform("sum")
        
        total_vals = df2['vals'].sum()
        
        # get percentage on node
        df2[f'%{node_col1}'] = df2[f'{node_col1}_sum'].apply(lambda x: x * 100/total_vals)
        df2[f'%{node_col2}'] = df2[f"{node_col2}_sum"].apply(lambda x: x * 100/total_vals)
        
        # get percentage on link
        df2[f"%{node_col2}/{node_col1}"] = round((df2['vals'] / df2[f"{node_col1}_sum"]) * 100)
        
        for _, row in df2.iterrows():
            if row[node_col1].split(_split_char)[-1] == "NC" and row[node_col2].split(_split_char)[-1] == "NC":
                continue
            
            # Add source, target, value
            sankey_input['sources'].append(label_map[row[node_col1]])
            sankey_input['targets'].append(label_map[row[node_col2]])
            sankey_input['values'].append(row['vals'])
            # 2. second col
            # Change displayed labels
            _percen_on_node = round(row[f'%{node_col2}'], 2)
            _percen_on_link = round(row[f"%{node_col2}/{node_col1}"], 2)
            _sum_on_node = row[f"{node_col2}_sum"]
            _node_str_name = row[node_col2]
            _node_index = label_map[_node_str_name]
            _split_char = '_'
            
            display_label = "" if _percen_on_node < show_threshold else f"({str(_percen_on_node)}%, {str(_sum_on_node)})"
            sankey_input["display_labels"][_node_index] = f"{_node_str_name.split(_split_char)[-1]} {display_label}"
            sankey_input['nodes_color'][_node_index] = minor_color if _percen_on_node < sankey_input["color_major_threshold"]['node'] else node_major_color
            
            # Add custom data
            sankey_input["links_percent"].append(_percen_on_link)
            sankey_input['links_color'].append(0 if row[node_col1].split(_split_char)[-1] == "NC" and row[node_col2].split(_split_char)[-1] == "NC" else 1)

            # Stage of display labels
            sankey_input["stage_nodes_value"][stage_counter][_node_index] = _sum_on_node
        
        # increase stage counter
        stage_counter+=1

    return sankey_input

def _get_annotations():
    annotations = []
    
    annotations.append(dict(
        x = -0.002,  # Position on the x-axis
        y = -0.05,  # Position on the y-axis
        xref='paper',  # Reference to the paper coordinates, not the data
        yref='paper',  # Reference to the paper coordinates
        text="Init",  # Text you want to display
        showarrow=False,  # Whether to show an arrow pointing to the annotation
        font=dict(size=15, color="#686D76", family="Arial"),  # Font properties
        align="center"  # Text alignment
    ))
    
    annotations.append(dict(
        x = -0.002 + 1/3 - 0.004,  # Position on the x-axis
        y = -0.05,  # Position on the y-axis
        xref='paper',  # Reference to the paper coordinates, not the data
        yref='paper',  # Reference to the paper coordinates
        text="Contract 1",  # Text you want to display
        showarrow=False,  # Whether to show an arrow pointing to the annotation
        font=dict(size=15, color="#686D76", family="Arial"),  # Font properties
        align="center"  # Text alignment
    ))
    
    annotations.append(dict(
        x = -0.002 + 2 * 1/3,  # Position on the x-axis
        y = -0.05,  # Position on the y-axis
        xref='paper',  # Reference to the paper coordinates, not the data
        yref='paper',  # Reference to the paper coordinates
        text="Contract 2",  # Text you want to display
        showarrow=False,  # Whether to show an arrow pointing to the annotation
        font=dict(size=15, color="#686D76", family="Arial"),  # Font properties
        align="center"  # Text alignment
    ))
    
    annotations.append(dict(
        x = -0.002 + 3 * 1/3 + 0.003,  # Position on the x-axis
        y = -0.05,  # Position on the y-axis
        xref='paper',  # Reference to the paper coordinates, not the data
        yref='paper',  # Reference to the paper coordinates
        text="Contract 3",  # Text you want to display
        showarrow=False,  # Whether to show an arrow pointing to the annotation
        font=dict(size=15, color="#686D76", family="Arial"),  # Font properties
        align="center"  # Text alignment
    ))
    
    return annotations

def _get_sankey_node_order(node_order, sankey_input):
    def _cal_x_axis(total_stage_num, cur_stage):
        x_axises = [0.001 + stage * 0.999/(total_stage_num - 1) for stage in range(total_stage_num)]
        return x_axises[cur_stage] if x_axises[cur_stage] != 1 else 0.999
        
    display_labels = sankey_input['display_labels']
    stage_nodes_value = sankey_input['stage_nodes_value']
    sankey_node_order = {
        "x_axises": {},
        "y_axises": {}
    }
    
    # get max stage height
    max_height = 0
    for _, stage in stage_nodes_value.items():
        cur_stage_height = 0
        for __, node_height in stage.items():
            cur_stage_height+=node_height
        max_height = max(max_height, cur_stage_height)
    
    # calculate xaxis
    for _node_index, _node_name in display_labels.items():
        # get cur_Stage
        cur_stage = None
        for stage in stage_nodes_value:
            for node in stage_nodes_value[stage]:
                if node == _node_index:
                    cur_stage = stage    
        # get x values
        sankey_node_order["x_axises"][_node_index] = _cal_x_axis(total_stage_num = len(stage_nodes_value), cur_stage = cur_stage)
        
    # calculate y values
    for istage, stage in stage_nodes_value.items():
        last_node_values = []
        # get stage_height
        stage_height = 0
        for node_value in stage.values(): stage_height += node_value
        # get x axis
        nodes_order = list(dict(sorted(node_order.items(), key=lambda item: item[1])).keys())
        # for first node
        first_prinode = nodes_order[0]
        _node_index = [_node_index for _node_index in stage if len(re.findall(first_prinode, display_labels[_node_index])) > 0][0]
        sankey_node_order['y_axises'][_node_index] = 0.001
        cur_node_value = stage[_node_index]
        last_node_values.append((0.001, cur_node_value))
        
        # for second node
        second_prinode = nodes_order[1]
        _node_index = [_node_index for _node_index in stage if len(re.findall(second_prinode, display_labels[_node_index])) > 0][0]
        last_node_value = last_node_values.pop()
        cur_node_xaxis = last_node_value[0] + (last_node_value[1] / stage_height) - 0.15
        cur_node_xaxis = cur_node_xaxis if cur_node_xaxis > 0.001 else last_node_value[0] + (last_node_value[1] / stage_height)
        cur_node_value = stage[_node_index]
        sankey_node_order['y_axises'][_node_index] = cur_node_xaxis
        last_node_values.append((cur_node_xaxis, cur_node_value))

        # for other node
        for prinode in nodes_order[2:]:
            try:
                _node_index = [_node_index for _node_index in stage if len(re.findall(prinode, display_labels[_node_index])) > 0][0]
            except Exception as e:
                print(e)
                continue
            
            last_node_value = last_node_values.pop()
            
            cur_node_xaxis = last_node_value[0] + (last_node_value[1] / stage_height)
            cur_node_value = stage[_node_index]
            
            sankey_node_order['y_axises'][_node_index] = cur_node_xaxis
            last_node_values.append((cur_node_xaxis, cur_node_value))
            
            
    
    return sankey_node_order 

def get_sankey_output(sankey_input):
    sources = sankey_input['sources']
    targets = sankey_input['targets']
    values = sankey_input['values']
    links_percent = sankey_input['links_percent']
    sorted_display_labels = dict(sorted(sankey_input['display_labels'].items()))
    sorted_x_axis = list(dict(sorted(sankey_input['sankey_node_order']['x_axises'].items())).values())
    sorted_y_axis = list(dict(sorted(sankey_input['sankey_node_order']['y_axises'].items())).values())
    
    
    # coloring
    nodes_color = dict(sorted(sankey_input['nodes_color'].items()))
    links_color = sankey_input['links_color']
    bg_color = "white"
    
    fig = go.Figure(go.Sankey(
    arrangement='snap',
    node = dict(
        pad = 15,
        thickness = 30,
        line = dict(color = "black", width = 0.5),
        label = list(sorted_display_labels.values()),  # Customized label
        hovertemplate = '%{value}<extra></extra>',
        color = _random_colors_list(sankey_input),
        x = sorted_x_axis,
        y = [
            0.001,
            0.7839185221334309,
            0.8459673641188312,
            0.8111354236564388,
            0.001,
            0.18651922568576018,
            0.2297897273226377,
            0.24802921978893427,
            0.1775723174525712,
            0.5502659468472315,
            0.3059120559588442,
            0.13029299091075458,
            0.001,
            0.15929927408040015,
            0.12681589702921978,
            0.1737058297240692,
            0.17765873645255088,
            0.001,
            0.05255655869375139,
            0.06926084304276214,
            0.07604219280587242,
            0.051001016694117406
        ]
        # y = sorted_y_axis
    ),
    link = dict(
        source = sources,
        target = targets,
        value = values,
        customdata = links_percent,
        hovertemplate = '%{customdata}%<extra></extra>',
        # color = [bg_color if i == 0 else "#A9A9A9" for i in links_color]
    )
    ))
    
    fig.update_layout(
        title={'text':'Channels Sankey Chart v3','y':0.95,'x':0.5,'xanchor':'center','yanchor':'top','font_size':30},
        font=dict(size=12, color="#3C3D37", family="Arial Black"),  # General font for labels
        plot_bgcolor="lightyellow",  # Plot background color
        paper_bgcolor=bg_color,   # Paper background color
        hoverlabel=dict(font_size=16, font_family="Helvetica"),  # Hover label font customization
        annotations = _get_annotations()
        # width =
        # height = 
    )

    return fig

if __name__ == "__main__":
    # READ
    df = pd.read_csv(os.path.join(os.getcwd(), "testdata.csv"))
    dfd = df[df['INI_SIGN_MONTH'].isin(["2021-06-01"])]
    
    # RENAME AND GET UINQUE NODES
    dfd['INI_CONTRACT_TYPE'] = dfd['INI_CONTRACT_TYPE'].apply(lambda x: f"INIT_{x}")
    dfd['CONTRACT1_TYPE'] = dfd['CONTRACT1_TYPE'].apply(lambda x: f"C1_{x}")
    dfd['CONTRACT2_TYPE'] = dfd['CONTRACT2_TYPE'].apply(lambda x: f"C2_{x}")
    dfd['CONTRACT3_TYPE'] = dfd['CONTRACT3_TYPE'].apply(lambda x: f"C3_{x}")

    unique_channels = list(pd.concat([dfd['INI_CONTRACT_TYPE'], dfd['CONTRACT1_TYPE'], dfd['CONTRACT2_TYPE'],  dfd['CONTRACT3_TYPE']]).unique())
    label_map = {channel: idx for idx, channel in enumerate(unique_channels)}
    
    # GET SANKEY INPUT:
    sankey_input = prepare_sankey(
        df, 
        node_col_list = ['INI_CONTRACT_TYPE', "CONTRACT1_TYPE", "CONTRACT2_TYPE", "CONTRACT3_TYPE"], 
        val_col = "CLIENTS", 
        val_agg = "sum", 
        show_threshold = 0,
        node_order = {
            "CD":0,
            "TW":1,
            "CL":2,
            "CC":3,
            "HPL":4,
            "NC":5
        }
    )
    
    ic(
        sankey_input['display_labels'],
        sankey_input['sankey_node_order']['x_axises'],
        sankey_input['sankey_node_order']['y_axises']
    )
    
    # GET SANKEY OUTPUT:
    fig = get_sankey_output(sankey_input)
    
    # LOAD SANKEY CHART
    fig.write_html(os.path.join(os.getcwd(), "plot.html"))