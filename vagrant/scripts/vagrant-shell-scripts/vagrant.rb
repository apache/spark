require 'erb'

def vagrant_shell_scripts_configure(shell, scripts_path, script_name = 'provision')
    shell.privileged = false
    shell.inline = ERB.new(<<-eos
<%

def import(file)
    ['#{ scripts_path }', '#{ File.dirname(__FILE__) }'].each do |path|
        if File.file?(File.join(path, file))
            return ERB.new(
                File.read(
                    File.join(path, file)
                )
            ).result(binding)
        end
    end
    raise ArgumentError, 'The file "' + file + '" cannot be imported.'
end

%>

<%= import '#{ script_name }' %>
    eos
    ).result
end