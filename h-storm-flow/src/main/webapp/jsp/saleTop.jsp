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
            src="../js/highcharts/themes/grid-light.js">
    </script>

    <script type="text/javascript">
        var series1;
        var series2;
        //var xAxi1 ;
        var xName;
        var NameArr;

        function jsFun(m) {
            //alert('aaaa');
            var jsdata = eval("(" + m + ")");

            var chart = $('#container').highcharts();
            var xNameStr = jsdata.xName;
            xNameStr = xNameStr.replace('[', '').replace(']', '');
            //alert(xNameStr);
            NameArr = xNameStr.split(',');
            //alert(NameArr[0]);
            chart.xAxis[0].setCategories(
                [NameArr[0], NameArr[1], NameArr[2], NameArr[3], NameArr[4]]
            );

            series1.setData(eval(jsdata.todayColumn));
            series2.setData(eval(jsdata.todaySpline));

        }

        function init() {
            var action = "<%=path%>/servlet/saleTop";
            $('#myForm').attr("action", action);
            $('#myForm').submit();
        }
    </script>

    <script type="text/javascript">
        $(function () {
            chart = new Highcharts.Chart({
                chart: {
                    zoomType: 'xy',
                    renderTo: 'container',
                    events: {
                        load: function () {
                            series1 = this.series[0];
                            series2 = this.series[1];
                            //xAxi1 = this.xAxis[0];
                            init();
                        }
                    }
                },
                title: {
                    text: '当日销售排行'
                },
                subtitle: {
                    text: 'Top 5'
                },
                xAxis: [{
                    categories: []
                }],
                yAxis: [{ // Primary yAxis
                    labels: {
                        format: '{value} 个',
                        style: {
                            color: '#89A54E'
                        },
                        overflow: 'justify'
                    },
                    title: {
                        text: '订单数',
                        style: {
                            color: '#89A54E'
                        }
                    },
                },
                    { // Secondary yAxis
                        title: {
                            text: '销售额',
                            style: {
                                color: '#4572A7'
                            }
                        },
                        labels: {
                            format: '{value} 元',
                            style: {
                                color: '#4572A7'
                            },
                            overflow: 'justify'
                        },
                        opposite: true
                    }],
                plotOptions: {
                    column: {
                        dataLabels: {
                            enabled: true
                        }
                    },
                    spline: {
                        dataLabels: {
                            enabled: true
                        }
                    }
                },
                tooltip: {
                    shared: true
                },
                legend: {
                    layout: 'vertical',
                    align: 'left',
                    x: 120,
                    verticalAlign: 'top',
                    y: 100,
                    floating: true,
                    backgroundColor: '#FFFFFF'
                },
                series: [{
                    name: '销售额',
                    color: '#4572A7',
                    type: 'column',
                    yAxis: 1,
                    data: [],
                    tooltip: {
                        valueSuffix: ' 元'
                    }

                }, {
                    name: '订单数',
                    color: '#89A54E',
                    type: 'spline',
                    data: [],
                    tooltip: {
                        valueSuffix: ' 个'
                    }
                }]
            });
        });

    </script>

</head>
<body>
<form method="post" id="myForm" action="" target="myiframe"></form>
<iframe id="myiframe" name="myiframe" style="display: none;"></iframe>


<div id="container" style="min-width: 300px; height: 600px"></div>
﻿

</body>
</html>