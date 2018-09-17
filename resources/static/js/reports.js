$(document).ready(function() {

    var dateFormat = "%Y/%m";
    var timeFormat = "%H:%M";
    var today = new Date();
    var yearone = 15;
    var yearlast = today.getFullYear() - 2000;
    var fy = yearlast + "/" + yearlast + 1;
    var fyselect = document.getElementById('fyselect');
    for (i=yearone;i<=yearlast;i++) {
	var fy1 = i;
	var fy2 = i+1;
	fy = fy1 + "/" + fy2;
	var opt = document.createElement('option');
	opt.value = fy;
	opt.innerHTML = fy;
	fyselect.appendChild(opt);
	fyselect.onchange = function() {
		fy = fyselect.value;
		updateData();
	}
    }
    var graphconfig = {
        graphs: {
            containerhistory: {label: "Gigabytes Archived", color: "#00FFFF", ylabel: "gigabytes", points: true},
            collectionsize: {label: "Collection Size in Gigabytes", color: "#00FFFF", ylabel: "gigabytes", points: true},
         }
    };

    var chart_options = {
        series: {
            lines: { show: true, fill: true },
            curvedLines: { apply: false, active: true, monotonicFit: true },
            points: {show: true},
        },
        legend: { show: true, position: "nw"},
        xaxis: { mode: "time", timeformat: "%Y/%m",timezone: "browser", minTickSize: [1, "month"], ticks: 11}, //, ticks: 12, minTicks: 12},
        yaxis: { zoomRange: [0.1, 10], panRange: [-10, 10], tickLength: 0,min: 0},
        zoom: { interactive: false},
        pan: { interactive: true},
        grid: { hoverable: true, clickable: true, color: "#000080",show:true}

    }

    var jsongood = function(gdata) {
        if (gdata == null) {
            return;
        }
        executeAsync(function() {
            var gname = gdata.label;
            if (graphconfig.graphs[gname].format) {
                chart_options.xaxis.timeformat=graphconfig.graphs[gname].format;
            } else {
                chart_options.xaxis.timeformat=dateFormat;
            }
            gdata.color=graphconfig.graphs[gname].color;
            gdata.label=graphconfig.graphs[gname].label;
            if (graphconfig.graphs[gname].points) {
                chart_options.series.points.show=true;
            } else {
                chart_options.series.points.show=false;
            }

            $("#graph-wrapper-"+gname).show();
            $.plot($("#gd_"+gname), [gdata], chart_options);
            $.unblockUI();
        });
    }

    function executeAsync(func) {
        setTimeout(func, 0);
    }

    function updateData() {
        for (var k in graphconfig.graphs) {
            var gname = k;
            $.ajax({
                type: 'get',
                dataType: 'json',
                async: true,
                url: 'reportdata?fy='+fy,
                data: {graph: gname},
                success: jsongood
            });
        }
    }

    function showToolTip(x, y, contents,g) {
            $('<div id="tooltip">' + contents + '</div>').css( {
                    position: 'absolute',
                    top: y,
                    left: x,
                    display: 'none',
                    border: '1px solid #fdd',
                    padding: '2px',
                    'background-color': '#fee',
                    opacity: 0.90
                }).appendTo("body").fadeIn(200);
                //}).appendTo("#gd_"+g).fadeIn(200);
    }
    var previousPoint = null;


    // mainish part
    //var offset = new Date().getTimezoneOffset();
    //var now = new Date();
    //var epoch = now.getTime();
    //console.log("Browser Time: "+now+" : "+epoch);
    //console.log("Browser TZ: "+offset);

    for (k in graphconfig.graphs) {
        $("#graph-wrapper-"+k).hide();

        $("#gd_"+k).bind("plothover", function (event, pos, item) {
            if (item) {
                value = pos.y.toFixed(0);
                time = pos.x.toFixed(0);
                time = item.datapoint[0].toFixed(0),

                x = item.pageX;
                y = item.pageY;
                if (previousPoint != item.dataIndex) {
                    previousPoint = item.dataIndex;
                    $("#tooltip").remove();
                    epoch = time / 1000;
                    d = new Date(0);
                    d.setUTCSeconds(epoch);
                    var contents = item.series.label + ": "+ value + "<br>" + d;
                    showToolTip(item.pageX,item.pageY,contents,k);
                }
            } else {
                $("#tooltip").remove();
                previousPoint = null;            
            }
        });
    }
    updateData();
    setInterval(updateData,300000);
});
