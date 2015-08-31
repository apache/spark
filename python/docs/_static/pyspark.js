$(function (){

    var sidebarLinkMap = function() {

        var linkMap = {};
        $('div.sphinxsidebar a.reference.internal').each(function (i,a)  {

            var href = $(a).attr('href');
            if (href && href.indexOf('#module-') == 0) {
                var id = href.substr(8);
                linkMap[id] = [$(a), null];
            }
        })

        return linkMap;
    }();


    $('dl.class > dt, dl.function > dt').each(function (i,dt)  {
        var id = dt.id;
        var desc = $(dt).find('> code.descname').text();

        if (id) {

            var last_idx = id.lastIndexOf('.');
            var mod_id = last_idx == -1? '': id.substr(0, last_idx);

            var r = sidebarLinkMap[mod_id];
            if (r) {
                if (r[1] === null) {
                    r[1] = $('<ul/>');
                    r[0].after(r[1]);
                }
                r[1].append('<li><a href="#' + id + '">' + desc + '</a></li>');
            }
        }
    });
});