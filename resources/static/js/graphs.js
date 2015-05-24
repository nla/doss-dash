$(document).ready(function() {

    var dateFormat = "%a %H";
    var timeFormat = "%H:%M";
    var graphconfig = {
        graphs: {
            todaysblobs: {label: "Todays Blob Archiving ", color: "#FF00FF", ylabel: "blobs", format: timeFormat},
            blobhistory: {label: "Blobs Archived", color: "#FF99FF", ylabel: "containers"},
            containerhistory: {label: "Containers Archived", color: "#00FFFF", ylabel: "containers"},
            auditcontainers: {label: "Containers Verified", color: "#44FF00", ylabel: "containers"},
            auditblobs: {label: "Blobs Verified", color: "#0044FF", ylabel: "blobs"},
         }
    };

    var chart_options = {
        series: {
            lines: { show: true, fill: true },
            curvedLines: { apply: true, active: true, monotonicFit: true }
        },
        legend: { show: true, position: "ne"},
        xaxis: { mode: "time", timeformat: "%Y/%m/%d %H:%M",timezone: "browser", ticks: 7, minTicks: 7},
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
                url: 'getdata',
                data: {graph: gname},
                success: jsongood
            });
        }
    }

    // mainish part
    //var offset = new Date().getTimezoneOffset();
    //var now = new Date();
    //var epoch = now.getTime();
    //console.log("Browser Time: "+now+" : "+epoch);
    //console.log("Browser TZ: "+offset);

    for (k in graphconfig.graphs) {
        $("#graph-wrapper-"+k).hide();
    }
    updateData();
    setInterval(updateData,300000);
});
