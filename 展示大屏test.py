from typing import List

import pyecharts.options as opts
from pyecharts.globals import ThemeType
from pyecharts.commons.utils import JsCode
from pyecharts.charts import Timeline, Grid, Bar, Map, Pie, Line

"""
Gallery 使用 pyecharts 1.3.0
From pyecharts 交流分享群 -- 郭昱
"""
data = [
    {
        "time": "2018年",
        "data": [
            {"name": "广东省", "value": [97278.0, 10.63, "广东省"]},
            {"name": "江苏省", "value": [92595.0, 10.12, "江苏省"]},
            {"name": "山东省", "value": [76470.0, 8.36, "山东省"]},
            {"name": "浙江省", "value": [56197.0, 6.14, "浙江省"]},
            {"name": "河南省", "value": [48056.0, 5.25, "河南省"]},
            {"name": "四川省", "value": [40678.0, 4.45, "四川省"]},
            {"name": "湖北省", "value": [39367.0, 4.3, "湖北省"]},
            {"name": "湖南省", "value": [36426.0, 3.98, "湖南省"]},
            {"name": "河北省", "value": [36010.0, 3.94, "河北省"]},
            {"name": "福建省", "value": [35804.0, 3.91, "福建省"]},
            {"name": "上海市", "value": [32680.0, 3.57, "上海市"]},
            {"name": "北京市", "value": [30320.0, 3.31, "北京市"]},
            {"name": "安徽省", "value": [30007.0, 3.28, "安徽省"]},
            {"name": "辽宁省", "value": [25315.0, 2.77, "辽宁省"]},
            {"name": "陕西省", "value": [24438.0, 2.67, "陕西省"]},
            {"name": "江西省", "value": [21985.0, 2.4, "江西省"]},
            {"name": "重庆市", "value": [20363.0, 2.23, "重庆市"]},
            {"name": "广西壮族自治区", "value": [20353.0, 2.23, "广西壮族自治区"]},
            {"name": "天津市", "value": [18810.0, 2.06, "天津市"]},
            {"name": "云南省", "value": [17881.0, 1.95, "云南省"]},
            {"name": "内蒙古", "value": [17289.0, 1.89, "内蒙古"]},
            {"name": "山西省", "value": [16818.0, 1.84, "山西省"]},
            {"name": "黑龙江省", "value": [16362.0, 1.79, "黑龙江省"]},
            {"name": "吉林省", "value": [15075.0, 1.65, "吉林省"]},
            {"name": "贵州省", "value": [14806.0, 1.62, "贵州省"]},
            {"name": "新疆维吾尔自治区", "value": [12199.0, 1.33, "新疆维吾尔自治区"]},
            {"name": "甘肃省", "value": [8246.0, 0.9, "甘肃省"]},
            {"name": "海南省", "value": [4832.0, 0.53, "海南省"]},
            {"name": "宁夏回族自治区", "value": [3705.0, 0.41, "宁夏回族自治区"]},
            {"name": "青海省", "value": [2865.0, 0.31, "青海省"]},
            {"name": "西藏自治区", "value": [1478.0, 0.16, "西藏自治区"]},
        ],
    },
]

time_list = [str(d) + "年" for d in range(1993, 2019)]

total_num = [
    3.4,
    4.5,
    5.8,
    6.8,
    7.6,
    8.3,
    8.8,
    9.9,
    10.9,
    12.1,
    14,
    16.8,
    19.9,
    23.3,
    28,
    33.3,
    36.5,
    43.7,
    52.1,
    57.7,
    63.4,
    68.4,
    72.3,
    78,
    84.7,
    91.5,
]
maxNum = 97300
minNum = 30


def get_year_chart():
    # 直接获取2018年的数据
    year = "2018年"
    year_data = [d["data"] for d in data if d["time"] == year]

    # 如果没有找到2018年数据，返回一个空的地图
    if not year_data:
        return None

    map_data = [[x["name"], x["value"]] for x in year_data[0]]
    min_data, max_data = (minNum, maxNum)
    data_mark: List = []
    i = 0
    for x in time_list:
        if x == year:
            data_mark.append(total_num[i])
        else:
            data_mark.append("")
        i = i + 1

    map_chart = (
        Map()
        .add(
            series_name="",
            data_pair=map_data,
            zoom=1,
            center=[119.5, 34.5],
            is_map_symbol_show=False,
            itemstyle_opts={
                "normal": {"areaColor": "#323c48", "borderColor": "#404a59"},
                "emphasis": {
                    "label": {"show": True},
                    "areaColor": "rgba(255,255,255, 0.5)",
                },
            },
        )
        .set_global_opts(
            title_opts=opts.TitleOpts(
                title=f"{year}全国分地区GDP情况（单位：亿） 数据来源：国家统计局",
                subtitle="",
                pos_left="center",
                pos_top="top",
                title_textstyle_opts=opts.TextStyleOpts(font_size=25, color="rgba(255,255,255, 0.9)"),
            ),
            tooltip_opts=opts.TooltipOpts(
                is_show=True,
                formatter=JsCode(
                    """function(params) {
                    if ('value' in params.data) {
                        return params.data.value[2] + ': ' + params.data.value[0];
                    }
                }"""
                ),
            ),
            visualmap_opts=opts.VisualMapOpts(
                is_calculable=True,
                dimension=0,
                pos_left="30",
                pos_top="center",
                range_text=["High", "Low"],
                range_color=["lightskyblue", "yellow", "orangered"],
                textstyle_opts=opts.TextStyleOpts(color="#ddd"),
                min_=min_data,
                max_=max_data,
            ),
        )
    )

    line_chart = (
        Line()
        .add_xaxis(time_list)
        .add_yaxis("", total_num)
        .add_yaxis(
            "",
            data_mark,
            markpoint_opts=opts.MarkPointOpts(data=[opts.MarkPointItem(type_="max")]),
        )
        .set_series_opts(label_opts=opts.LabelOpts(is_show=False))
        .set_global_opts(
            title_opts=opts.TitleOpts(title="全国GDP总量1993-2018年（单位：万亿）", pos_left="72%", pos_top="5%")
        )
    )
    bar_x_data = [x[0] for x in map_data]
    bar_y_data = [{"name": x[0], "value": x[1][0]} for x in map_data]
    bar = (
        Bar()
        .add_xaxis(xaxis_data=bar_x_data)
        .add_yaxis(
            series_name="",
            y_axis=bar_y_data,
            label_opts=opts.LabelOpts(is_show=True, position="right", formatter="{b} : {c}"),
        )
        .reversal_axis()
        .set_global_opts(
            xaxis_opts=opts.AxisOpts(max_=maxNum, axislabel_opts=opts.LabelOpts(is_show=False)),
            yaxis_opts=opts.AxisOpts(axislabel_opts=opts.LabelOpts(is_show=False)),
            tooltip_opts=opts.TooltipOpts(is_show=False),
            visualmap_opts=opts.VisualMapOpts(
                is_calculable=True,
                dimension=0,
                pos_left="10",
                pos_top="top",
                range_text=["High", "Low"],
                range_color=["lightskyblue", "yellow", "orangered"],
                textstyle_opts=opts.TextStyleOpts(color="#ddd"),
                min_=min_data,
                max_=max_data,
            ),
        )
    )

    pie_data = [[x[0], x[1][0]] for x in map_data]
    pie = (
        Pie()
        .add(
            series_name="",
            data_pair=pie_data,
            radius=["15%", "35%"],
            center=["80%", "82%"],
            itemstyle_opts=opts.ItemStyleOpts(
                border_width=1, border_color="rgba(0,0,0,0.3)"
            ),
        )
        .set_global_opts(
            tooltip_opts=opts.TooltipOpts(is_show=True, formatter="{b} {d}%"),
            legend_opts=opts.LegendOpts(is_show=False),
        )
    )

    grid_chart = (
        Grid()
        .add(
            bar,
            grid_opts=opts.GridOpts(pos_left="10", pos_right="45%", pos_top="50%", pos_bottom="5"),
        )
        .add(
            line_chart,
            grid_opts=opts.GridOpts(pos_left="65%", pos_right="80", pos_top="10%", pos_bottom="50%"),
        )
        .add(pie, grid_opts=opts.GridOpts(pos_left="45%", pos_top="60%"))
        .add(map_chart, grid_opts=opts.GridOpts())
    )

    return grid_chart


if __name__ == "__main__":
    timeline = Timeline(
        init_opts=opts.InitOpts(width="1600px", height="900px", theme=ThemeType.DARK)
    )

    # 直接生成2018年图表
    g = get_year_chart()
    if g:
        timeline.add(g, time_point="2018年")

    timeline.add_schema(
        orient="vertical",
        is_auto_play=True,
        is_inverse=True,
        play_interval=5000,
        pos_left="null",
        pos_right="5",
        pos_top="20",
        pos_bottom="20",
        width="60",
        label_opts=opts.LabelOpts(is_show=False, color="#fff"),
    )

    timeline.render("china_gdp_2018.html")
