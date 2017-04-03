var waitState = false;
var listening = false;

function ucfirst(string) {
    if (typeof string != 'string')
        return null;
    return string.substr(0, 1).toUpperCase() + string.substr(1);
}

function interval(iv) {
    var days = Math.floor(iv / 86400);
    var hours = Math.floor(iv / 60 / 60) - (days * 24);
    var minutes = Math.floor(iv / 60) - (hours * 60) - (days * 60 * 24);
    var seconds = Math.floor(iv) - (minutes * 60) - (hours * 60 * 60) - (days * 60 * 60 * 24);
    return String("00" + days).slice(-2) + ':' + String("00" + hours).slice(-2) + ':' + String("00" + minutes).slice(-2) + ':' + String("00" + seconds).slice(-2);
}

function uptime() {
    var ut = $('#uptime');
    var start = ut.attr('data-start');
    if (start > 0) {
        var secs = Math.round(((new Date()).getTime() / 1000) - start);
        ut.html(interval(secs));
    } else {
        ut.html('');
    }
}

function pad(n, width, z) {
    z = z || '0';
    n = n + '';
    return n.length >= width ? n : new Array(width - n.length + 1).join(z) + n;
}

function formatDate(date) {
    return date.getFullYear()
        + '-' + pad(date.getMonth() + 1, 2)
        + '-' + pad(date.getDate(), 2)
        + ' ' + pad(date.getHours(), 2)
        + ':' + pad(date.getMinutes(), 2)
        + ':' + pad(date.getSeconds(), 2);
}

function mem(size) {
    return (size / Math.pow(2, 20)).toFixed(2)
}

function updateStatus(stats) {
    var btnState = $('#btnService').is('[on=true]');
    if (stats.state == 'running' && !btnState) {
        if (waitState == true) waitState = null;
        $('#btnService').attr('on', 'true');
    } else if (stats.state == 'stopped' && btnState) {
        if (waitState == false) waitState = null;
        $('#btnService').attr('on', 'false');
    }
    $('#status-state').html(ucfirst(stats.state));
    $('#uptime').attr('data-start', stats.started);
    $('#status-pid').html(stats.pid);
    $('#status-memory').html(mem(stats.memory));
    $('#status-connections').html(stats.connections);
    $('#status-clients').html(stats.clients);
    for (prop in stats.stats) {
        $('#status-' + prop).html(stats.stats[prop]);
    }
    uptime();
}

function displayPanel(parent, id, label, data, suffix) {
    var panel = parent.find('#panel_' + id)
    if (panel.length == 0) {
        var table = $('<table>');
        for (field in data) {
            table.append($('<tr data-field="' + field + '">').append($('<th>').html(field), $('<td>').html(data[field])));
        }
        panel = $('<div class="panel">').attr('id', 'panel_' + id)
            .append($('<div class="panel-label">').html(label), table);
        parent.append(panel.append(suffix));
    }
    return panel;
}

function updatePanel(parent, id, data) {
    var panel = parent.find('#panel_' + id);
    panel.find('table tbody').children('tr[data-field]').each(function (index, item) {
        var field = $(item).attr('data-field');
        if (data[field]) $(item).children('td').html(data[field]);
    });
    return panel;
}

function removePanel(parent, id) {
    parent.find('#panel_' + id).remove();
}

function handleTrigger(data, event) {
    switch (data.args.type) {
        case 'job':
            var job = data.args.job;
            var parent = $('#joblist');
            if (data.command == 'add') {
                var info = {
                    'Status': String(job.status_text).toUpperCase(),
                    'ID': job.id,
                    'Start': (new Date(job.start * 1000)).toLocaleString(),
                    'Environment': job.application.env,
                    'Retries': job.retries
                };
                displayPanel(parent, data.args.id, (job.tag ? job.tag : job.id), info);
            } else if (data.command == 'update') {
                var info = {
                    'Status': String(job.status_text).toUpperCase(),
                    'Started': (new Date(job.start * 1000)).toLocaleString(),
                    'Retries': job.retries
                };
                updatePanel(parent, data.args.id, info);
            } else if (data.command == 'remove') {
                removePanel(parent, data.args.id);
            }
            break;
        case 'process':
            var parent = $('#processlist');
            var proc = data.args.process;
            if (data.command == 'add') {
                var info = {
                    'State': 'Starting',
                    'PID': proc.pid,
                    'Started': (new Date(proc.start)).toLocaleString(),
                    'Environment': proc.env,
                    'Memory': '0 MB',
                    'Peak': '0 MB'
                };
                displayPanel(parent, data.args.id, data.args.process.tag, info);
            } else if (data.command == 'update') {
                var info = {
                    'State': proc.status.state,
                    'PID': proc.status.pid,
                    'Memory': mem(proc.status.mem) + ' MB',
                    'Peak': mem(proc.status.peak) + ' MB'
                };
                updatePanel(parent, data.args.id, info);
            } else if (data.command == 'remove') {
                removePanel(parent, data.args.id);
            }
            break;
        case 'client':
            var parent = $('#clientlist');
            var client = data.args.client;
            if (data.command == 'add') {
                var host = client.ip ? client.ip + ':' + client.port : 'Internal';
                var label = client.username ? client.username : host;
                var since = new Date(client.since * 1000);
                var dinfoata = {
                    'ID': client.id,
                    'Host': host,
                    'User': (client.username ? client.username : 'None'),
                    'Since': formatDate(since)
                };
                displayPanel(parent, client.id, label, info)
                    .toggleClass('client-admin', client.admin)
                    .toggleClass('client-system', client.system);
            } else if (data.command == 'update') {
                updatePanel(parent, client.id, client).toggleClass('client-admin', client.admin);
            } else if (data.command == 'remove') {
                removePanel(parent, data.args.client);
            }
            break;
        case 'event':
            var parent = $('#eventlist');
            var e = data.args.event;
            if (data.command == 'add') {
                var info = {
                    'ID': e.trigger,
                    'When': (new Date(e.when * 1000)).toLocaleString(),
                    'Data': JSON.stringify(e.data)
                };
                displayPanel(parent, e.trigger, e.id, info);
            } else if (data.command = 'remove') {
                removePanel(parent, data.args.id);
            }
            break;
        case 'service':
            var parent = $('#servicelist');
            var service = data.args.service;
            if (!service.name)
                break;
            if (data.command == 'update') {
                var info = {
                    'Status': service.status,
                    'Restarts': service.restarts,
                    'Heartbeats': service.heartbeats,
                    'Last Heartbeat': (new Date(service.last_heartbeat * 1000)).toLocaleString()
                };
                var panel = parent.find('#panel_' + service.name);
                if (panel.length == 0) {
                    var button = $('<button class="btnToggleService">').html((service.enabled) ? 'Disable' : 'Enable');
                    displayPanel(parent, service.name, service.name, info, button);
                } else {
                    updatePanel(parent, service.name, info);
                }
            }
            break;
        default:
            console.log("Event type '" + data.args.type + "' not implemented yet!");
            break;
    }
    updateStatus(data.status);
}

$.fn.dialog = function (params) {
    if (!params.title)
        params.title = 'Dialog box';
    var host = this.get(0);
    if (this.parent().length == 0)
        this.appendTo('body');
    host.winDIV = $('<div class="dialog">').attr('data-close', 'true');
    host.titleDIV = $('<div class="dialog-title">').html(params.title);
    host.contentDIV = $('<div class="dialog-content">');
    host._close = function (event, action) {
        if (typeof params.onClose == 'function') {
            params.onClose(action);
        }
        host.winDIV.fadeOut(function () {
            if ($(this).attr('data-close') == 'true')
                $(this).remove();
        });
    }
    this.addClass('content-text');
    host.buttonsDIV = $('<div class="buttons">');
    if (!params.buttons) {
        params.buttons = [
            {
                label: 'OK', class: 'primary', action: 'cancel'
            }
        ];
    }
    for (i in params.buttons) {
        var meta = params.buttons[i];
        if (!meta.label)
            meta.label = 'OK';
        if (!meta.class)
            meta.class = 'default';
        var button = $('<button>').html(meta.label).addClass(meta.class);
        host.buttonsDIV.append(button);
        if (typeof meta.action == 'function') {
            button.click($.proxy(meta.action, this)).click(function (event) {
                host._close(event, this.action);
            });
        } else {
            button.get(0).action = meta.action;
            button.click(function (event) {
                host._close(event, this.action);
            });
        }
        if (meta.default) {
            button.addClass('action').attr('data-default', true);
        }
    }
    host.winDIV.keypress(function (event) {
        if (event.keyCode == 13)
            $(this).find('button[data-default="true"]').click();
    });
    this.after(host.winDIV);
    host.winDIV.append(host.titleDIV, host.contentDIV.append(this, host.buttonsDIV)).css({
        top: (window.innerHeight / 2) - (host.winDIV.height() / 2),
        left: (window.innerWidth / 2) - (host.winDIV.width() / 2)
    }).fadeIn();
    return host.winDIV;
};

(function ($) {
    $.fn.reOrder = function (array) {
        return this.each(function () {

            if (array) {
                for (var i = 0; i < array.length; i++)
                    array[i] = $('div[category="' + array[i] + '"]');

                $(this).empty();

                for (var i = 0; i < array.length; i++)
                    $(this).append(array[i]);
            }
        });
    }
})(jQuery);

$(document).ready(function () {
    /*$('#btnService').click(function (event) {
        var url = 'warlock/';
        waitState = $(this).attr('on') == 'false';
        if (waitState) {
            url += 'start';
        } else {
            url += 'stop';
        }
        $.get(url).done(function (data) {
            if (data.result != 'ok') {
                alert('Command failed!');
                $(event.target).jqxSwitchButton('toggle');
            }
        });
    });*/

    warlock.sync(hazaar.get('admin_key'));

    warlock.subscribe(hazaar.get('admintrigger'), handleTrigger)
        .onconnect(function () {
            warlock.status();
        })
        .onerror(function () {
            $('table tbody tr').remove();
        }).onclose(function () {
            $('table tbody tr').remove();
        }).onstatus(function (data) {
            console.log('got status');
            updateStatus(data);
        });

    setInterval(function () {
        uptime();
    }, 1000);

    setInterval(function () {
        if (warlock.connected())
            warlock.status();
    }, 30000);

});
