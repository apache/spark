/**
 * Created by janomar on 23/07/15.
 */
  function toggleJdbc(connectionType) {
       isJdbc = connectionType == 'jdbc'
       $("#port").parent().parent().toggleClass('hide', isJdbc)
       $("#schema").parent().parent().toggleClass('hide', isJdbc)
       $("#extra").parent().parent().toggleClass('hide', isJdbc)
       $("#jdbc_drv_clsname").parent().parent().toggleClass('hide', !isJdbc)
       $("#jdbc_drv_path").parent().parent().toggleClass('hide', !isJdbc)

       if (isJdbc) {
           $("label[for='host']").text("Connection URL");
       } else {
            $("label[for='host']").text("Host");
       }
  }

  jQuery(document).ready(function() {
      var conn_type =jQuery("#conn_type").val();
      jQuery("#conn_type").on('change', function(e) {
        conn_type = jQuery("#conn_type").val();
        toggleJdbc(conn_type);
      });
      toggleJdbc(conn_type);
});