<%@ page language="java" import="java.util.*" pageEncoding="UTF-8"%>
<%
    String path=this.getServletContext().getContextPath();
%>
<html>
<head>
    <title>非二跳UV</title>
    <script type="text/javascript" src="../js/jquery/jquery-1.7.1.min.js"></script>
    <script src="../js/highcharts/highcharts.js"></script>
    <script src="../js/highcharts/modules/exporting.js"></script>
    <script type="text/javascript">
        var chart;
        var series;
        var series1;
        var series2;
        var initFlg = true;

        var lst_x=0;
        var xTitleShow="";
        var isShowTip=false;//用于控制只在addpoint的时候才对tip框进行操作

        function msg(m){
            showFrameData(m);
        }
    </script>
    <script type="text/javascript">
        function initShow(){
            //alert('init');
            var act = "<%=path%>/servlet/uvSvlt";

            $('#showForm').attr("action", act);
            $('#showForm').submit();
        }

        //展示数据
        function showFrameData(m){
            var jsonData = eval("("+m+")");//转换为json对象

            //跨天处理
            var isNewDay = jsonData.isNewDay;
            if(isNewDay==1){
                initFlg = true;
                //清空
                series.remove(false);
                series1.remove(false);
                series2.remove(false);
                chart.redraw();
                //重绘
                chart.addSeries({
                    id:1,
                    name: "当前UV",
                    color: '#FF0000',
                    type: 'spline',
                    data: [[0,0]]
                },false);
                chart.addSeries({
                    id:2,
                    name: "上月同期UV",
                    color: '#00B050',
                    type: 'spline',
                    data: [[0,0]]
                },false);
                var seriestt=chart.series;
                series = seriestt[0];
                series1 = seriestt[1];
                series2 = seriestt[2];
                //跨天之后3秒刷新一次页面，为的是使dataLabels生效
                setInterval(function() {
                    location.reload(true);
                }, 3000);
            }

            if(initFlg){
                series.addPoint([0,0], true, false);
                series1.addPoint([0,0], true, false);
                series.setData(eval(jsonData.initData));
                series1.setData(eval(jsonData.initHisData));
                series2.setData(eval(jsonData.initHourData));
                initFlg = false;
            }else{
                //持续加点
                var option_={name:jsonData.name, x:jsonData.x, y:jsonData.y};
                lst_x=jsonData.x;
                xTitleShow = jsonData.name;
                series.addPoint(option_, true, false);
                series2.setData(eval(jsonData.initHourData));
                isShowTip = true;
            }
        }

        $(function () {
            //alert('chart');
            chart = new Highcharts.Chart({
                chart: {
                    renderTo: 'container',
                    events: {
                        load: function() {
                            series = this.series[0];
                            series1 = this.series[1];
                            series2 = this.series[2];
                            //初始化调用
                            initShow();
                        }
                    }
                },
                title: {
                    text: '实时非二跳UV统计',
                },
                xAxis: {
                    min:0,
                    max:24,
                    categories: ['00','01', '02', '03', '04', '05', '06', '07', '08', '09', '10', '11', '12'
                        , '13', '14', '15', '16', '17', '18', '19', '20', '21', '22', '23', '24'],
                    labels:{
                        step:1,
                    }
                },
                yAxis: [{ // Primary yAxis
                    min : 0,
                    labels: {
                        //enabled: false,
                        overflow : 'justify'
                    }
                }],
                tooltip: {
                    valueDecimals: 0,
                },
                plotOptions: {
                    series:{
                        turboThreshold:0,
                    },
                    spline: {
                        lineWidth: 4,
                        marker: {
                            enabled: false
                        }
                    },
                    column: {
                        lineWidth: 4,
                        marker: {
                            enabled: false
                        },
                        dataLabels : {
                            enabled : true
                        }
                    }
                },
                series: [{
                    name: '当天UV',
                    color: '#FF0000',
                    legendIndex: 2,
                    type: 'spline',
                    cursor: 'pointer',
                    dataLabels:{
                        enabled:true, //开启数据标签
                        align: 'left',
                        borderRadius: 5,
                        borderWidth: 1,
                        borderColor: '#AAA',
                        //x: 10,
                        //y: -6,
                        //padding: 6,//内容距离边框的大小
                        //shadow: true,
                        style: {
                            fontWeight: 'bold'
                        },
                        useHTML: true,
                        formatter: function () {
                            if(lst_x==this.point.x&&isShowTip){
                                var showTipValue = (this.point.y).toFixed(0);//去小数点
                                showTipValue = commafy(showTipValue);//转为千分位，如123,4560
                                //if(xTitleShow.indexOf('.')!=-1){
                                //	xTitleShow = xTitleShow.replace(/\./g, ":");
                                //}
                                return '<div id=tmplst><b>时间&nbsp;' + xTitleShow + "</b><br/>UV&nbsp;" + showTipValue + '</div>';
                            }
                        }
                    },
                    data: [],
                    //tooltip: {
                    //}
                }, {
                    name: '上月同期UV',
                    color: '#00B050',
                    legendIndex: 1,
                    type: 'spline',
                    data: [],
                    tooltip: {
                    }
                },
                    {
                        name: '小时分布',
                        color: 'blue',
                        width: 100,
                        legendIndex: 3,
                        type: 'column',
                        data: [],
                        tooltip: {
                        }
                    }]
            });
        });


        /**
         * 数字格式转换成千分位
         *@param{Object}num
         */
        function commafy(num) {
            //1.先去除空格,判断是否空值和非数
            num = num + "";
            num = num.replace(/[ ]/g, ""); //去除空格
            if (num == "") {
                return;
            }
            if (isNaN(num)) {
                return;
            }
            //2.针对是否有小数点，分情况处理
            var index = num.indexOf(".");
            if (index == -1) { //无小数点
                var reg = /(-?\d+)(\d{3})/;
                while (reg.test(num)) {
                    num = num.replace(reg, "$1,$2");
                }
            } else {
                var intPart = num.substring(0, index);
                var pointPart = num.substring(index + 1, num.length);
                var reg = /(-?\d+)(\d{3})/;
                while (reg.test(intPart)) {
                    intPart = intPart.replace(reg, "$1,$2");
                }
                num = intPart + "." + pointPart;
            }
            return num;
        }
    </script>

    <script src="../js/highcharts/themes/skies.js"></script>
    <script src="../js/highcharts/common.js"></script>
</head>
<body>
<form id="showForm" action="" target="myiframe" method="post">
</form>
<iframe name="myiframe" id="myiframe" style="display: none;"></iframe>
<div id="container" style="min-width: 510px; height: 400px; margin: 0 auto"></div>
</body>
</html>