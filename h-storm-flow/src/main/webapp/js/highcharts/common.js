/** * Dark blue theme for Highcharts JS * @author Torstein Honsi */
Highcharts.theme = {
    colors: ["#2CA02C", "#D41E1F", "#808080"],
    chart: {
        animation: {
                duration: 1000
            },
        height:738,
        width:1004,
    },
    /*legend: {  
        align: 'left',  
        x: 250,  
        verticalAlign: 'top',  
        y: 35,  
        floating: true,  
        backgroundColor: (Highcharts.theme && Highcharts.theme.legendBackgroundColorSolid) || 'white',  
        borderColor: '#CCC',  
        borderWidth: 1,  
        shadow: false  
    }, */
    credits:{
		//href:'http://www.yihaodian.com',
		//text:'yihaodian'
		enabled: false,
	},
	exporting:{
		enabled: false,
	},
	yAxis: {
		title:null,
    },
}; // Apply the theme 
var highchartsOptions = Highcharts.setOptions(Highcharts.theme);