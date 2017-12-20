<%@ page language="java" pageEncoding="UTF-8" %>
<%
    String path = request.getContextPath();
    String basePath = request.getScheme() + "://"
            + request.getServerName() + ":" + request.getServerPort()
            + path + "/";
%>

<html lang="en">
<head>
    <script type="text/javascript"
            src="../js/jquery/jquery-1.7.1.min.js">
    </script>
    <script type="text/javascript"
            src="../js/highcharts/highcharts.js">
    </script>
    <script type="text/javascript"
            src="../js/highcharts/modules/exporting.js">
    </script>
    <script type="text/javascript"
            src="../js/highcharts/highcharts-3d.js">
    </script>
    <script type="text/javascript"
            src="../js/highcharts/themes/grid-light.js">
    </script>

    <script type="text/javascript">
        var series1;
        var series2;

        function jsFun(m) {
            var jsdata = eval("(" + m + ")");
            //alert(jsdata);
            series1.setData(eval(jsdata.todayData));
            series2.setData(eval(jsdata.hisData));
        }

        function init() {
            var action = "<%=path%>/servlet/AreaSvlt";
            $('#myForm').attr("action", action);
            $('#myForm').submit();
        }
    </script>

    <script type="text/javascript">
        $(function () {
            var chart = new Highcharts.Chart({
                chart: {
                    renderTo: 'container',
                    type: 'column',
                    events: {
                        load: function () {
                            series1 = this.series[0];
                            series2 = this.series[1];
                            init();
                        }
                    },
                    margin: 75,
                    options3d: {
                        enabled: true,
                        alpha: 15,
                        beta: 15,
                        depth: 50,
                        viewDistance: 25
                    }
                },
                title: {
                    text: '地区实时金额'
                },
                subtitle: {
                    text: '按天统计'
                },
                plotOptions: {
                    column: {
                        dataLabels: {
                            enabled: true
                        }
                    }
                },
                xAxis: {
                    categories: ['北京', '上海', '广州', '深圳', '成都']
                },
                yAxis: {
                    min: 0,
                    labels: {
                        overflow: 'justify'
                    }
                },
                series: [
                    {
                        name: '当前',
                        color: '#41A8BE',
                        legendIndex: 2,
                        index: 2,
                        data: []
                    },
                    {
                        name: '上月同期',
                        color: '#808080',
                        legendIndex: 3,
                        index: 4,
                        data: []
                    }]
            });


            // Activate the sliders
            $('#R0').on('change', function () {
                chart.options.chart.options3d.alpha = this.value;
                showValues();
                chart.redraw(false);
            });
            $('#R1').on('change', function () {
                chart.options.chart.options3d.beta = this.value;
                showValues();
                chart.redraw(false);
            });

            function showValues() {
                $('#R0-value').html(chart.options.chart.options3d.alpha);
                $('#R1-value').html(chart.options.chart.options3d.beta);
            }

            showValues();
        });

    </script>
</head>
<body>
<form method="post" id="myForm" action="" target="myiframe"></form>
<iframe id="myiframe" name="myiframe" style="display: none;"></iframe>


<div id="container" style="min-width: 400px; height: 400px"></div>
﻿
<div id="sliders"
     style="min-width: 310px; max-width: 800px; margin: 0 auto;">
    <table>
        <tr>
            <td>
                Alpha Angle
            </td>
            <td>
                <input id="R0" type="range" min="0" max="45" value="15"/>
                <span id="R0-value" class="value"></span>
            </td>
        </tr>
        <tr>
            <td>
                Beta Angle
            </td>
            <td>
                <input id="R1" type="range" min="0" max="45" value="15"/>
                <span id="R1-value" class="value"></span>
            </td>
        </tr>
    </table>
</div>
</body>
</html>