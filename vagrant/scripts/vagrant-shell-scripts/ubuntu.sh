#!/usr/bin/env bash

set -e

[ -n "${SUDO-x}" ] && SUDO='sudo'

ERROR_BOLD="\e[1;31m"
ERROR_NORMAL="\e[0;31m"
DEBUG_BOLD="\e[1;35m"
DEBUG_NORMAL="\e[0;35m"
RESET="\e[00m"

if [[ -n "$COLORS" ]] && [[ ! "$COLORS" =~ ^(always|yes|true|1)$ ]]; then
  unset ERROR_BOLD
  unset ERROR_NORMAL
  unset DEBUG_BOLD
  unset DEBUG_NORMAL
  unset RESET
fi

# {{{ Utils

# Return the value of the first argument or exit with an error message if empty.
script-argument-create() {
  [ -z "$1" ] && {
    echo -e "${ERROR_BOLD}E: ${ERROR_NORMAL}You must specify $2 to '${BASH_SOURCE[0]}'.${RESET}" 1>&2
    exit 1
  }
  echo "$1"
}

# Log an operation
log-operation() {
  local function_name
  local function_values
  local arg
  function_name="$1"
  shift
  for arg in "$@"; do
    function_values="$function_values ""'$( echo "$arg" | sed -e 's#\s\+# #g' )'"
  done
  [ -z "$QUIET" ] && echo -e "${DEBUG_BOLD}$function_name${DEBUG_NORMAL}(""$( echo "$function_values" | sed -e 's#^ ##' -e "s#\s\+''\$##g" )"")...${RESET}"
}

# }}}

# {{{ Nameservers

# Drop all local 10.0.x.x nameservers in 'resolv.conf'.
nameservers-local-purge() {
  log-operation "$FUNCNAME" "$@"
  $SUDO sed -e 's#nameserver\s*10\.0\..*$##g' -i '/etc/resolv.conf'
}

# Set up an IP as a DNS name server if not already present in 'resolv.conf'.
nameservers-append() {
  log-operation "$FUNCNAME" "$@"
  grep "$1" '/etc/resolv.conf' >/dev/null || \
    ( echo "nameserver $1" | $SUDO tee -a '/etc/resolv.conf' >/dev/null )
}

# }}}

# {{{ Aptitude

# Set up a specific two-letter country code as the preferred `aptitude` mirror.
apt-mirror-pick() {
  log-operation "$FUNCNAME" "$@"
  $SUDO sed -i \
    -e "s#\w\+\.archive\.ubuntu\.com#$1.archive.ubuntu.com#g" \
    -e "s#security\.ubuntu\.com#$1.archive.ubuntu.com#g" \
    '/etc/apt/sources.list'
}

# Add a custom repository as a software source.
apt-packages-repository() {
  log-operation "$FUNCNAME" "$@"
  dependency-install 'add-apt-repository'
  while [[ "$1" =~ ^deb ]] || [[ "$1" =~ ^ppa ]]; do
    $SUDO add-apt-repository -y "$( echo "$1" | sed -e 's#^deb-src\b#deb#' )" 1>/dev/null
    # See https://bugs.launchpad.net/ubuntu/+source/software-properties/+bug/972617
    if [[ "$1" =~ ^deb ]] && [[ ! "$1" =~ ^deb-src ]]; then
      $SUDO add-apt-repository --remove -y "$( echo "$1" | sed -e 's#^deb\b#deb-src#' )" 1>/dev/null
    fi
    shift
  done
  if [ -n "$1" ] && ! $SUDO apt-key list | grep -q "$1"; then
    $SUDO apt-key adv -q --keyserver "${2:-keyserver.ubuntu.com}" --recv-keys "$1" 1>/dev/null
  fi
}

# Add a Launchpad PPA as a software source.
apt-packages-ppa() {
  local ppa_repository="$1"
  shift
  # If the repository is already set up, don't re-add it.
  if ! apt-cache policy | grep "$ppa_repository" 1>/dev/null 2>&1; then
    apt-packages-repository "ppa:${ppa_repository}" "$@"
  fi
}

# Perform a non-interactive `apt-get` command.
apt-non-interactive() {
  log-operation "$FUNCNAME" "$@"
  $SUDO                                    \
    DEBIAN_FRONTEND=noninteractive         \
    apt-get                                \
      -o Dpkg::Options::='--force-confdef' \
      -o Dpkg::Options::='--force-confold' \
      -f -y -qq                            \
      --no-install-recommends              \
      "$@"
}

# Update `aptitude` packages without any prompts.
apt-packages-update() {
  apt-non-interactive update
}

# Perform an unattended installation of package(s).
apt-packages-install() {
  apt-non-interactive install "$@"
}

# Perform an unattended complete removal (purge) of package(s).
apt-packages-purge() {
  local result
  local code
  result=$( apt-non-interactive -q purge "$@" 2>&1 ) || {
    code=$?
    # If no packages matched, it's OK.
    if [[ ! "$result" =~ "E: Couldn't find package" ]]; then
      echo "$result" 1>&2
      exit $code
    fi
  }
  # Take care of any leftovers.
  apt-non-interactive autoremove
}

# }}}

# {{{ System

# Run a complete system upgrade.
system-upgrade() {
  apt-non-interactive upgrade
}

# Command a system service, e.g., apache2, mysql, etc.
system-service() {
  log-operation "$FUNCNAME" "$@"
  $SUDO service "$1" "$2" 1>/dev/null
}

# Escape and normalize a string so it can be used safely in file names, etc.
system-escape() {
  local glue
  glue=${1:--}
  while read arg; do
    echo "${arg,,}" | sed -e 's#[^[:alnum:]]\+#'"$glue"'#g' -e 's#^'"$glue"'\+\|'"$glue"'\+$##g'
  done
}

# }}}

# {{{ Default Commands

# Update the Ruby binary link to point to a specific version.
alternatives-ruby-install() {
  log-operation "$FUNCNAME" "$@"
  local bin_path
  local man_path
  bin_path="${2:-/usr/bin/}"
  man_path="${3:-/usr/share/man/man1/}"
  $SUDO update-alternatives                                                         \
    --install "${bin_path}ruby"      ruby      "${bin_path}ruby$1"      "${4:-500}" \
    --slave   "${man_path}ruby.1.gz" ruby.1.gz "${man_path}ruby$1.1.gz"             \
    --slave   "${bin_path}ri"        ri        "${bin_path}ri$1"                    \
    --slave   "${bin_path}irb"       irb       "${bin_path}irb$1"                   \
    --slave   "${bin_path}rdoc"      rdoc      "${bin_path}rdoc$1"
  $SUDO update-alternatives --verbose                                               \
    --set                            ruby      "${bin_path}ruby$1"
}

# Create symbolic links to RubyGems binaries.
alternatives-ruby-gems() {
  log-operation "$FUNCNAME" "$@"
  local ruby_binary
  local ruby_version
  local binary_name
  local binary_path
  ruby_binary=$( $SUDO update-alternatives --query 'ruby' | grep 'Value:' | cut -d' ' -f2- )
  ruby_version="${ruby_binary#*ruby}"
  if grep -v '^[0-9.]*$' <<< "$ruby_version"; then
    echo "E: Could not determine version of RubyGems."
  fi
  for binary_name in "$@"; do
    binary_path="/var/lib/gems/$ruby_version/bin/$binary_name"
    $SUDO update-alternatives --install "$( dirname "$ruby_binary" )/$binary_name" "$binary_name" "$binary_path" 500
    $SUDO update-alternatives --verbose --set                                      "$binary_name" "$binary_path"
  done
}

# }}}

# {{{ Apache

# Enable a list of Apache modules. This requires a server restart.
apache-modules-enable() {
  log-operation "$FUNCNAME" "$@"
  $SUDO a2enmod $* 1>/dev/null
}

# Disable a list of Apache modules. This requires a server restart.
apache-modules-disable() {
  log-operation "$FUNCNAME" "$@"
  $SUDO a2dismod $* 1>/dev/null
}

# Enable a list of Apache sites. This requires a server restart.
apache-sites-enable() {
  log-operation "$FUNCNAME" "$@"
  $SUDO a2ensite $* 1>/dev/null
}

# Disable a list of Apache sites. This requires a server restart.
apache-sites-disable() {
  log-operation "$FUNCNAME" "$@"
  $SUDO a2dissite $* 1>/dev/null
}

# Create a new Apache site and set up Fast-CGI components.
apache-sites-create() {
  log-operation "$FUNCNAME" "$@"
  local apache_site_name
  local apache_site_path
  local apache_site_user
  local apache_site_group
  local apache_site_config
  local apache_verbosity
  local cgi_action
  local cgi_apache_path
  local cgi_system_path
  local code_block
  apache_site_name="$1"
  apache_site_path="${2:-/$apache_site_name}"
  apache_site_user="${3:-$apache_site_name}"
  apache_site_group="${4:-$apache_site_user}"
  apache_verbosity="${5:-info}"
  apache_site_config="/etc/apache2/sites-available/$apache_site_name"
  cgi_apache_path="/cgi-bin/"
  cgi_system_path="$apache_site_path/.cgi-bin/"
  # Create the /.cgi-bin/ directory and set permissions for SuExec.
  $SUDO mkdir -p "$cgi_system_path"
  $SUDO chmod 0755 "$cgi_system_path"
  # Define a new virtual host with mod_fastcgi configured to use SuExec.
  code_block=$( cat <<-EOD
<IfModule mod_fastcgi.c>
  FastCgiWrapper /usr/lib/apache2/suexec
  FastCgiConfig  -pass-header HTTP_AUTHORIZATION -autoUpdate -killInterval 120 -idle-timeout 30
</IfModule>

<VirtualHost *:80>
  DocumentRoot ${apache_site_path}

  LogLevel ${apache_verbosity}
  ErrorLog /var/log/apache2/error.${apache_site_name}.log
  CustomLog /var/log/apache2/access.${apache_site_name}.log combined

  SuexecUserGroup ${apache_site_user} ${apache_site_group}
  ScriptAlias ${cgi_apache_path} ${cgi_system_path}

  # Do not use kernel sendfile to deliver files to the client.
  EnableSendfile Off

  <Directory ${apache_site_path}>
    Options All
    AllowOverride All
  </Directory>
EOD
  )
  # Is PHP required?
  if [ ! -z "$PHP" ]; then
    cgi_action="php-fcgi"
    code_block=$( cat <<-EOD
${code_block}

  <IfModule mod_fastcgi.c>
    <Location ${cgi_apache_path}${cgi_action}>
      SetHandler fastcgi-script
      Options +ExecCGI +FollowSymLinks
      Order Allow,Deny
      Allow from all
    </Location>
    AddHandler ${cgi_action} .php
    Action     ${cgi_action} ${cgi_apache_path}${cgi_action}
  </IfModule>
EOD
    )
    $SUDO cat > "$cgi_system_path$cgi_action" <<-EOD
#!/bin/bash

export PHP_FCGI_CHILDREN=4
export PHP_FCGI_MAX_REQUESTS=200

export PHPRC="${cgi_system_path}php.ini"

exec ${PHP}
EOD
    $SUDO chmod 0755 "$cgi_system_path$cgi_action"
  fi
  code_block=$( cat <<-EOD
${code_block}
${EXTRA}
</VirtualHost>
EOD
  )
  # Write site configuration to Apache.
  echo "$code_block" | $SUDO tee "$apache_site_config" >/dev/null
  # Configure permissions for /.cgi-bin/ and SuExec.
  $SUDO chown -R "$apache_site_user":"$apache_site_group" "$cgi_system_path"
  # Update SuExec to accept the new document root for this website.
  grep "$apache_site_path" '/etc/apache2/suexec/www-data' >/dev/null || \
    ( $SUDO sed -e '1s#^#'"$apache_site_path""\n"'#' -i '/etc/apache2/suexec/www-data' >/dev/null )
}

# Restart the Apache server and reload with new configuration.
apache-restart() {
  system-service apache2 restart
}

# }}}

# {{{ Nginx

# Figure out the path to a particular Nginx site.
nginx-sites-path() {
  echo "/etc/nginx/sites-${2:-available}/$1"
}

# Enable a list of Nginx sites. This requires a server restart.
nginx-sites-enable() {
  log-operation "$FUNCNAME" "$@"
  local name
  local file
  for name in "$@"; do
    file="$( nginx-sites-path "$name" 'enabled' )"
    if [ ! -L "$file" ]; then
      # '-f'orce because '! -L' above would still evaluate for broken symlinks.
      $SUDO ln -fs "$( nginx-sites-path "$name" 'available' )" "$file"
    fi
  done
}

# Disable a list of Nginx sites. This requires a server restart.
nginx-sites-disable() {
  log-operation "$FUNCNAME" "$@"
  local name
  local file
  for name in "$@"; do
    file="$( nginx-sites-path "$name" 'enabled' )"
    if [ -L "$file" ]; then
      $SUDO unlink "$file"
    fi
  done
}

# Create a new Nginx site and set up Fast-CGI components.
nginx-sites-create() {
  log-operation "$FUNCNAME" "$@"
  local nginx_site_name
  local nginx_site_path
  local nginx_site_index
  local nginx_site_user
  local nginx_site_group
  local nginx_verbosity
  local nginx_site_config
  local code_block
  nginx_site_name="$1"
  nginx_site_path="${2:-/$nginx_site_name}"
  nginx_site_user="${3:-$nginx_site_name}"
  nginx_site_group="${4:-$nginx_site_user}"
  nginx_site_index="${5:-index.html}"
  nginx_verbosity="${6:-info}"
  nginx_site_config="$( nginx-sites-path "$nginx_site_name" 'available' )"
  # Is PHP required?
  if [ ! -z "$PHP" ]; then
    if ! which php5-fpm >/dev/null; then
      echo 'E: You must install php5-fpm to use PHP in Nginx.' 1>&2
      exit 1
    fi
    nginx_site_index="index.php $nginx_site_index"
  fi
  code_block=$( cat <<-EOD
server {
  listen 80;

  root ${nginx_site_path};

  error_log /var/log/nginx/error.${nginx_site_name}.log ${nginx_verbosity};
  access_log /var/log/nginx/access.${nginx_site_name}.log combined;

  index ${nginx_site_index};

  # Do not use kernel sendfile to deliver files to the client.
  sendfile off;

  # Prevent access to hidden files.
  location ~ /\. {
    access_log off;
    log_not_found off;
    deny all;
  }
EOD
  )
  # Is PHP required?
  if [ ! -z "$PHP" ]; then
    code_block=$( cat <<-EOD
${code_block}

  # Pass PHP scripts to PHP-FPM.
  location ~ \.php\$ {
    include         fastcgi_params;
    fastcgi_pass    unix:/var/run/php5-fpm.${nginx_site_name}.sock;
    fastcgi_index   index.php;
    fastcgi_split_path_info ^((?U).+\.php)(/?.+)\$;
    fastcgi_param PATH_INFO \$fastcgi_path_info;
    fastcgi_param PATH_TRANSLATED \$document_root\$fastcgi_path_info;
    fastcgi_param HTTP_AUTHORIZATION \$http_authorization;
  }
EOD
    )
    # Run PHP-FPM as the selected user and group.
    $SUDO sed \
      -e 's#^\[www\]$#['"$nginx_site_name"']#g'                                            \
      -e 's#^\(user\)\s*=\s*[A-Za-z0-9-]\+#\1 = '"$nginx_site_user"'#g'                    \
      -e 's#^\(group\)\s*=\s*[A-Za-z0-9-]\+#\1 = '"$nginx_site_group"'#g'                  \
      -e 's#^\(listen\)\s*=\s*.\+$#\1 = /var/run/php5-fpm.'"$nginx_site_name"'.sock#g'     \
      < '/etc/php5/fpm/pool.d/www.conf' | $SUDO tee '/etc/php5/fpm/pool.d/'"$nginx_site_name"'.conf' >/dev/null
  fi
  code_block=$( cat <<-EOD
${code_block}
${EXTRA}
}
EOD
  )
  # Write site configuration to Nginx.
  echo "$code_block" | $SUDO tee "$nginx_site_config" >/dev/null
}

# Restart the Nginx server and reload with new configuration.
nginx-restart() {
  system-service nginx restart
}

# }}}

# {{{ PHP

# Update a PHP setting value in all instances of 'php.ini'.
php-settings-update() {
  log-operation "$FUNCNAME" "$@"
  local args
  local settings_name
  local php_ini
  local php_extra
  args=( "$@" )
  PREVIOUS_IFS="$IFS"
  IFS='='
  args="${args[*]}"
  IFS="$PREVIOUS_IFS"
  settings_name="$( echo "$args" | system-escape )"
  for php_ini in $( $SUDO find /etc -type f -iname 'php*.ini' ); do
    php_extra="$( dirname "$php_ini" )/conf.d"
    $SUDO mkdir -p "$php_extra"
    echo "$args" | $SUDO tee "$php_extra/0-$settings_name.ini" >/dev/null
  done
}

# Install (download, build, install) and enable a PECL extension.
php-pecl-install() {
  log-operation "$FUNCNAME" "$@"
  local specification extension
  local mode
  dependency-install 'make'
  dependency-install 'pecl'
  dependency-install 'phpize'
  for specification in "$@"; do
    extension="${specification%@*}"
    if ! $SUDO pecl list | grep "^${extension}" >/dev/null; then
      $SUDO pecl install -s "${specification/@/-}" 1>/dev/null
    fi
    # Special case for Zend extensions.
    if [[ "$extension" =~ ^(xdebug)$ ]]; then
      mode="zend_extension"
      extension="$( php-config --extension-dir )/${extension}"
    else
      mode="extension"
    fi
    php-settings-update "$mode" "${extension}.so"
  done
}

# Restart the PHP5-FPM server.
php-fpm-restart() {
  system-service php5-fpm restart
}

# }}}

# {{{ MySQL

# Create a database if one doesn't already exist.
mysql-database-create() {
  log-operation "$FUNCNAME" "$@"
  mysql -u root -e "CREATE DATABASE IF NOT EXISTS \`$1\` CHARACTER SET ${2:-utf8} COLLATE '${3:-utf8_general_ci}'"
}

# Restore a MySQL database from an archived backup.
mysql-database-restore() {
  log-operation "$FUNCNAME" "$@"
  local backups_database
  local backups_path
  local backups_file
  local tables_length
  backups_database="$1"
  backups_path="$2"
  if [ -d "$backups_path" ]; then
    tables_length=$( mysql -u root --skip-column-names -e "USE '$backups_database'; SHOW TABLES" | wc -l )
    if [ "$tables_length" -lt 1 ]; then
      backups_file=$( find "$backups_path" -maxdepth 1 -type f -regextype posix-basic -regex '^.*[0-9]\{8\}-[0-9]\{4\}.tar.bz2$' | \
        sort -g | \
        tail -1 )
      if [ ! -z "$backups_file" ]; then
        tar -xjf "$backups_file" -O | mysql -u root "$backups_database"
      fi
    fi
  fi
}

# Allow remote passwordless 'root' access for anywhere.
# This is only a good idea if the box is configured in 'Host-Only' network mode.
mysql-remote-access-allow() {
  log-operation "$FUNCNAME" "$@"
  mysql -u root -e "GRANT ALL PRIVILEGES ON *.* TO 'root'@'%' IDENTIFIED BY '' WITH GRANT OPTION; FLUSH PRIVILEGES;"
  $SUDO sed -e 's#127.0.0.1#0.0.0.0#g' -i '/etc/mysql/my.cnf'
}

# Restart the MySQL server and reload with new configuration.
mysql-restart() {
  system-service mysql restart
}

# }}}

# {{{ RubyGems

# Check the installed version of a package.
ruby-gems-version() {
  $SUDO ruby -rubygems -e "puts Gem::Specification::find_by_name('$1').version" 2>/dev/null || \
    echo '0.0.0'
}

# Perform an unattended installation of a package.
ruby-gems-install() {
  log-operation "$FUNCNAME" "$@"
  local gem_version
  gem_version="$( ruby-gems-version "$1" )"
  if [[ "$gem_version" = '0.0.0' ]]; then
    $SUDO gem install --no-ri --no-rdoc "$@"
  fi
}

# }}}

# {{{ NPM (Node Package Manager)

# Perform an unattended **global** installation of package(s).
npm-packages-install() {
  log-operation "$FUNCNAME" "$@"
  $SUDO npm config set yes true
  $SUDO npm install -g $*
}

# }}}

# {{{ Oracle JDK (Oracle Java Development Kit)

# Install Oracle JDK to /opt/java, update defaults and add it to PATH
oracle-jdk-install() {
  log-operation "$FUNCNAME" "$@"
  dependency-install 'curl'
  local cookie
  local jdk_link 
  local target
  cookie="$1"
  jdk_link="$2"  
  temp_dir="$3" 
  temp_out_file="$temp_dir"/oracle_jdk.tar.gz
  # download if the file is not exists
  if [ ! -f "$temp_out_file" ]; then
    curl -L --progress-bar --header "$cookie" "$jdk_link" -z "$temp_out_file" -o "$temp_out_file"
  fi

  tar -xf "$temp_out_file" -C "$temp_dir"
  $SUDO mkdir -p /opt/
  $SUDO mv "$temp_dir"/jdk* /opt/java

  $SUDO update-alternatives --install "/usr/bin/java" "java" "/opt/java/bin/java" 1
  $SUDO update-alternatives --install "/usr/bin/javac" "javac" "/opt/java/bin/javac" 1
  $SUDO update-alternatives --install "/usr/bin/javaws" "javaws" "/opt/java/bin/javaws" 1

  env-append 'JAVA_HOME' "/opt/java/"
  env-append 'PATH' "/opt/java/bin/"
}

# }}}

# {{{ GitHub

# Download and install RubyGems from GitHub.
github-gems-install() {
  log-operation "$FUNCNAME" "$@"
  local repository
  local clone_path
  local configuration
  local gem_version
  dependency-install 'git'
  which 'gem' >/dev/null || {
    echo 'E: Please install RubyGems to continue.' 1>&2
    exit 1
  }
  for repository in "$@"; do
    configuration=(${repository//@/"${IFS}"})
    gem_version="$( ruby-gems-version "${configuration[0]#*\/}" )"
    if [[ "$gem_version" = '0.0.0' ]]; then
      clone_path="$( mktemp -d -t 'github-'$( echo "${configuration[0]}" | system-escape )'-XXXXXXXX' )"
      git clone --progress "git://github.com/${configuration[0]}" "$clone_path"
      (                                                   \
        cd "$clone_path"                               && \
        git checkout -q "${configuration[1]:-master}"  && \
        gem build *.gemspec                            && \
        ruby-gems-install *.gem                           \
      )
      rm -Rf "$clone_path"
    fi
  done
}

# Install (download, build, install) and enable a PECL extension.
github-php-extension-install() {
  log-operation "$FUNCNAME" "$@"
  local specification repository version arguments extension
  local raw_config_uri clone_path
  # We must have 'phpize' available when compiling extensions from source.
  dependency-install 'git'
  dependency-install 'curl'
  dependency-install 'phpize'
  for specification in "$@"; do
    repository="$specification"
    version='master'
    # If we have a space anywhere in the string, assume `configure` arguments.
    if [[ "$repository" =~ ' ' ]]; then
      arguments=( ${repository#* } )
      repository="${repository%% *}"
    fi
    # If we have a version specified, split the repository name and version.
    if [[ "$repository" =~ '@' ]]; then
      version="${repository#*@}"
      repository="${repository%@*}"
    fi
    # Let's figure out the extension name by looking at the autoconf file.
    raw_config_uri="https://raw.github.com/${repository}/${version}/config.m4"
    extension="$( curl --insecure --silent "$raw_config_uri" | grep -e 'PHP_NEW_EXTENSION' | cut -d'(' -f2 | cut -d',' -f1 | tr -d ' ' )"
    if [ -z "$extension" ]; then
      echo -e "${ERROR_BOLD}E: ${ERROR_NORMAL}Cannot find extension name in ${raw_config_uri}, expected 'PHP_NEW_EXTENSION'.${RESET}" 1>&2
      exit 1
    fi
    # We can't use PECL to determine if the extension is installed
    # so let's look for the .so file in the PHP extensions directory.
    if ! $SUDO ls -1 "$( php-config --extension-dir )" | grep "^${extension}.so$" >/dev/null; then
      clone_path="$( mktemp -d -t 'github-'$( echo "${repository}" | system-escape )'-XXXXXXXX' )"
      # Clone, configure, make, install... the usual.
      git clone --progress "git://github.com/${repository}" "$clone_path"
      (                                  \
        cd "$clone_path"              && \
        git checkout -q "$version"    && \
        phpize                        && \
        ./configure "${arguments[@]}" && \
        make && $SUDO make install       \
      )
      # Clean up and explicitly load the extension.
      rm -Rf "$clone_path"
      php-settings-update 'extension' "${extension}.so"
    fi
  done
}

# }}}

# {{{ Environment

env-append() {
  local env_key="$1" env_lookup=1 env_value="$2" env_comment="$3" env_line
  log-operation "$FUNCNAME" "$@"
  for profile_env in '/etc/profile' '/etc/zsh/zshenv'; do
    if [ -f "$profile_env" ]; then
      if [ "$env_key" == 'PATH' ]; then
        env_line="export ${env_key}='${env_value}':"'"$PATH"'
      elif [[ "$env_key" =~ ^(source|eval)$ ]]; then
        env_line="${env_key} ${env_value} || :"
      else
        env_line="[ -z "'"$'"$env_key"'"'" ] && export ${env_key}='${env_value}'"
      fi
      eval "$env_line" || :
      if ! grep "${env_key}.*${env_value}" "$profile_env" &>/dev/null; then
        echo -e "\n# AUTO-GENERATED: ${env_key}$( echo ", ${env_comment}" | sed -e 's/^,\s*$//' ).\n${env_line}" | $SUDO tee -a "$profile_env" 1>/dev/null
      fi
    fi
  done
}

# }}}

# {{{ Dependency Management

# Associate a package name with a command, e.g., 'git' <- 'git-core'.
dependency-package-associate() {
  local variable_name
  variable_name="DEPENDENCY_$(echo "$1" | system-escape '_' | tr '[a-z]' '[A-Z]' )"
  # If a second argument was specified...
  if [ -n "$2" ]; then
    # ...and a package name hasn't been associated with the command yet...
    if [ -z "${!variable_name}" ]; then
      # ...create a new association.
      eval $variable_name=\""$2"\"
    fi
  else
    echo "${!variable_name}"
  fi
}

# Satisfy a dependency by installing the associated package.
dependency-install() {
  local binary_name
  local package_name
  local apt_update_required=0
  local apt_update_performed=0
  local apt_update_datetime apt_update_timestamp apt_update_ago
  local timestamp_now
  for binary_name in "$@"; do
    which "$binary_name" >/dev/null || {
      package_name="$( dependency-package-associate "$binary_name" )"
      # If we cannot find the package name in the cache, request an update.
      # This may happen if `apt-get update` was not run on the machine before.
      if ! $SUDO apt-cache pkgnames | grep "$package_name" 1>/dev/null 2>&1; then
        apt_update_required=1
      fi
      # If the last time we updated the cache was more than a day ago, request an update.
      # This is needed to prevent errors when trying to install an out-of-date package from the cache.
      if [[ $apt_update_required -lt 1 ]] && [[ $apt_update_performed -lt 1 ]]; then
        # Determine the last date/time any lists files were modified, newest first.
        apt_update_datetime="$( \
          ls -lt --time-style='long-iso' '/var/lib/apt/lists/' 2>/dev/null | grep -o '\([0-9]\{2,4\}[^0-9]\)\{3\}[0-9]\{2\}:[0-9]\{2\}' -m 1 || \
          date +'%Y-%m-%d %H:%M'
        )"
        # Convert the YYYY-MM-DD HH:MM format to a Unix timestamo.
        apt_update_timestamp=$( date --date="$apt_update_datetime" +'%s' 2>/dev/null || echo 0 )
        # Calculate the number of seconds that have passed since the newest update.
        timestamp_now=$( date +'%s' )
        apt_update_ago=$(( $timestamp_now-$apt_update_timestamp ))
        if [ $apt_update_ago -gt 86400 ]; then
          apt_update_required=1
        fi
      fi
      # If the cache needs updating, do so before installing the package (once per call only).
      if [[ $apt_update_required -gt 0 ]] && [[ $apt_update_performed -lt 1 ]]; then
        apt_update_required=0
        apt_update_performed=1
        apt-packages-update
      fi
      apt-packages-install "$package_name"
    }
  done
}

# Create associations for packages we are going to install.
dependency-package-associate 'add-apt-repository' 'python-software-properties'
dependency-package-associate 'curl' 'curl'
dependency-package-associate 'git' 'git-core'
dependency-package-associate 'make' 'build-essential'
dependency-package-associate 'pecl' 'php-pear'
dependency-package-associate 'phpize' 'php5-dev'

# }}}
