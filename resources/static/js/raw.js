$(document).ready(function() {

    var dateFormat = "%Y/%m";
    var timeFormat = "%H:%M";
    var today = new Date();
    var fy = today.getYear();

    var jsongood = function(gdata) {
        if (gdata == null) {
            return;
        }
        executeAsync(function() {
            var data = gdata.data;
	    var mydiv = document.getElementById('rawdata');
            var table = document.createElement('table');
            var header = table.createTHead();
            var hr = header.insertRow();
            var th1 =hr.insertCell();
            th1.appendChild(document.createTextNode("Date"));
            var th2 =hr.insertCell();
            th2.appendChild(document.createTextNode("Gigabytes"));
            for (var i=0;i < data.length;i++) {
                var year = data[i];
                var ts = year[0];
                var num = year[1];
                var epoch = ts / 1000;
                var d = new Date(0);
                d.setUTCSeconds(epoch);
                var tr = table.insertRow();
                var td = tr.insertCell();
                td.appendChild(document.createTextNode((d.getMonth()+1) + "/" + d.getFullYear()));
                var td2 = tr.insertCell();
                td2.appendChild(document.createTextNode(num));
            }
            mydiv.appendChild(table);
            $.unblockUI();
        });
    }

    function executeAsync(func) {
        setTimeout(func, 0);
    }

    function updateData() {
            $.ajax({
                type: 'get',
                dataType: 'json',
                async: true,
                url: 'reportdata?fy='+fy,
                data: {graph: "collectionsize"},
                success: jsongood
            });
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

    updateData();
    setInterval(updateData,300000);
});
