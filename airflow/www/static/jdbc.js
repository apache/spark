/**
 * Created by janomar on 23/07/15.
 */
  //ugly, refactor soon
  function toggleJdbc(connectionType) {
       if (connectionType == 'jdbc') {
           $("input[name='port']").parent().parent().hide();
           $("input[name='schema']").parent().parent().hide();
           //$("input[name='login']").parent().parent().hide();
           //$("input[name='password']").parent().parent().hide();
           $("input[name='extra']").parent().parent().hide();
           $("input[name='jdbc_drv_clsname']").parent().parent().show();
           $("input[name='jdbc_drv_path']").parent().parent().show();
           $("label[for='host']").text("Connection URL");
       } else {
            $("input[name='port']").parent().parent().show();
            $("input[name='schema']").parent().parent().show();
            //$("input[name='login']").parent().parent().show();
            //$("input[name='password']").parent().parent().show();
            $("input[name='extra']").parent().parent().show();
            $("input[name='jdbc_drv_clsname']").parent().parent().hide();
            $("input[name='jdbc_drv_path']").parent().parent().hide();
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