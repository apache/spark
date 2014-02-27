vagrant-shell-scripts
=====================

A collection of scripts (Ubuntu-only at this time) to ease Vagrant box provisioning using the [shell][shell] method.

  [shell]: http://vagrantup.com/v1/docs/provisioners/shell.html

Usage
-----

1. Place on top of your `Vagrantfile` before `Vagrant.configure(..)`:

    ```ruby
    require File.join(File.dirname(__FILE__), 'path/to/vagrant-shell-scripts/vagrant')
    ```

2. Place at the end of your `Vagrantfile` before the last `end`:

    ```ruby
    config.vm.provision :shell do |shell|
        vagrant_shell_scripts_configure(
            shell,
            File.dirname(__FILE__),
            'path/to/provisioning_script'
        )
    end
    ```

3. Place on top of your provisioning script:

    ```bash
    #!/usr/bin/env bash

    <%= import 'ubuntu.sh' %>
    ```

Functions
---------

### Utility

- `script-argument-create(value, expected)`

    Return the value of the first argument or exit with an error message if empty.

    Example:

    ```bash
    MYSQL_DBNAME=$( script-argument-create "$1" 'a MySQL database name as the first argument' )
    ```

### Nameservers

- `nameservers-local-purge`

    Drop all local `10.0.x.x` nameservers in `resolv.conf`.

- `nameservers-append(ip)`

    Set up an IP as a DNS name server if not already present in `resolv.conf`.

    Example (set up Google DNS):

    ```bash
    nameservers-local-purge
    nameservers-append '8.8.8.8'
    nameservers-append '8.8.4.4'
    ```

### Aptitude

- `apt-mirror-pick(iso2)`

    Set up a specific two-letter country code as the preferred `aptitude` mirror.

    Example (use Ubuntu Bulgaria as a mirror):

    ```bash
    apt-mirror-pick 'bg'
    ```

- `apt-packages-repository(repository[, repository[, ...]][, key[, server = 'keyserver.ubuntu.com']])`

    Add a custom repository as a software source.

    Each `repository` is a line as it would appear in your `/etc/apt/sources.list` file, e.g., <nobr>`deb URL DISTRIBUTION CATEGORY`</nobr>.

    `key` and `server` are the signing key and server.
    You need to add the key to your system so Ubuntu can verify the packages from the repository.

    Example (install MongoDB from official repositories):

    ```
    apt-packages-repository 'deb http://downloads-distro.mongodb.org/repo/ubuntu-upstart dist 10gen' '7F0CEB10'
    apt-packages-update
    apt-packages-install mongodb-10gen
    ```

- `apt-packages-ppa(repository[, key[, server = 'keyserver.ubuntu.com']])`

    Add a Launchpad PPA as a software source.

    The `repository` is the Launchpad user and project name, e.g., `chris-lea/node.js`.

    `key` and `server` are the signing key and server.
    The key needs to added to your system so Ubuntu can verify the packages from the PPA.

    Example (install Node.js from unofficial PPA):

    ```bash
    apt-packages-ppa 'chris-lea/node.js'
    apt-packages-update
    apt-packages-install \
      nodejs             \
      npm
    ```

- `apt-packages-update`

    Update `aptitude` packages without any prompts.

- `apt-packages-install(package[, package[, ...]])`

    Perform an unattended installation of package(s).

    Example:

    ```bash
    apt-packages-update
    apt-packages-install     \
      apache2-mpm-worker     \
      apache2-suexec-custom  \
      mysql-server-5.1       \
      libapache2-mod-fastcgi \
      php5-cgi
    ```

- `apt-packages-purge(package[, package[, ...]])`

    Perform an unattended complete removal (purge) of package(s).

    Example (purge `apache2` unnecessarily installed as part of the `php5` meta-package):

    ```bash
    apt-packages-update
    apt-packages-install php5
    apt-packages-purge 'apache2*'
    ```

### System

- `system-upgrade()`

    Run a complete system upgrade.
    Be extremely careful as this operation can break packages, e.g., VirtualBox Guest Additions if a new kernel is installed.

    Example:

    ```bash
    apt-packages-update
    system-upgrade
    ```

- `system-service(name, action)`

    Command a system service, e.g., `apache2`, `mysql`, etc.

    Example:

    ```bash
    system-service php5-fpm restart
    ```

- `system-escape([glue = '-'])`

    Escape and normalize a string so it can be used safely in file names, etc.
    You can optionally specify `glue` to be a different character, e.g., an underscore `_` if you are using the result as part of a variable name.

    Example:

    ```bash
    echo "Hello World!" | system-escape  # prints 'hello-world'
    ```

### Default Commands

- `alternatives-ruby-install(version[, bin_path = '/usr/bin/'[, man_path = '/usr/share/man/man1/'[, priority = 500]]])`

    Update the Ruby binary link to point to a specific version.

    Example:

    ```bash
    apt-packages-install \
      ruby1.9.1          \
      ruby1.9.1-dev      \
      rubygems1.9.1
    alternatives-ruby-install 1.9.1
    ```

- `alternatives-ruby-gems()`

    Create symbolic links to RubyGems binaries.

    By default, RubyGems on Ubuntu does not create binaries on `$PATH`.
    Using this function would create a symbolic link in the directory of your `ruby` binary which is assumed to be on `$PATH`.

    Example (install stable versions of Sass and link it as `/usr/bin/sass`):

    ```bash
    apt-packages-install \
      ruby1.9.1          \
      ruby1.9.1-dev      \
      rubygems1.9.1
    alternatives-ruby-install 1.9.1
    github-gems-install 'nex3/sass'
    alternatives-ruby-gems 'sass'
    ```

### Apache

- `apache-modules-enable(module[, module[, ...]])`

    Enable a list of Apache modules. This requires a server restart.

- `apache-modules-disable(module[, module[, ...]])`

    Disable a list of Apache modules. This requires a server restart.

- `apache-sites-enable(name[, name[, ...]])`

    Enable a list of Apache sites. This requires a server restart.

- `apache-sites-disable(name[, name[, ...]])`

    Disable a list of Apache sites. This requires a server restart.

- `[PHP=/path/to/binary] apache-sites-create(name[, path = name[, user = name[, group = user[, verbosity = info]]]])`

    Create a new Apache site and set up Fast-CGI components.

    The function creates a new file under `sites-available/name` where `name` is the first argument to the function.

    To set up Fact-CGI, a new directory `.cgi-bin` is created in `path` if it doesn't already exist.

    The virtual host is pointed to `path` or `/name` if `path` is omitted.

    **SuExec is required** and a user and group options are set up so permissions can work properly.

    If you prefix the function with `PHP=/path/to/binary`, PHP will be enabled through a Fast-CGI script in `.cgi-bin`. You must have PHP installed.

    Example (create a new `vagrant` website from `/vagrant` and enable PHP):

    ```bash
    PHP=/usr/bin/php-cgi apache-sites-create 'vagrant'
    apache-sites-enable vagrant
    apache-restart
    ```

- `apache-restart`

    Restart the Apache server and reload with new configuration.

    Example:

    ```bash
    apache-modules-enable actions rewrite fastcgi suexec
    apache-restart
    ```

### Nginx

- `nginx-sites-enable(name[, name[, ...]])`

    Enable a list of Nginx sites. This requires a server restart.

- `nginx-sites-disable(name[, name[, ...]])`

    Disable a list of Nginx sites. This requires a server restart.

- `[PHP=any-value] nginx-sites-create(name[, path = name[, user = name[, group = user[, index = 'index.html'[, verbosity = info]]]]])`

    Create a new Nginx site and set up Fast-CGI components.

    The function creates a new file under `sites-available/name` where `name` is the first argument to the function.

    The virtual host is pointed to `path` or `/name` if `path` is omitted.

    If you prefix the function with `PHP=any-value`, PHP will be enabled through a PHP-FPM. You must have `php5-fpm` installed.

    The arguments `user` and `group` are used to re-configure PHP-FPM's default `www` pool to run under the given account.

    You can optionally specify space-separated directory index files to look for if a file name wasn't specified in the request.

    Example (create a new `vagrant` website from `/vagrant` and enable PHP):

    ```bash
    PHP=/usr/bin/php5-fpm nginx-sites-create 'vagrant'
    nginx-sites-enable vagrant
    nginx-restart
    ```

### PHP

- `php-settings-update(name, value)`

    Update a PHP setting value.
    This function will look for all `php.ini` files in `/etc`.
    For each file, a `conf.d` directory would be created in the parent directory (if one doesn't already exist) and inside a file specifying the setting name/value will be placed.

    Example (create a default timezone):

    ```bash
    php-settings-update 'date.timezone' 'Europe/London'
    ```

- `php-pecl-install(extension[, extension[, ...]])`

    Install (download, build, install) and enable a PECL extension.

    You may optionally specify the state/version using '@version'.

    Example (install MongoDB driver):

    ```bash
    apt-packages-install \
      php5-dev           \
      php-pear
    php-pecl-install mongo

    # Install a specific version of 'proctitle'.
    php-pecl-install proctitle@0.1.2
    ```

- `php-fpm-restart`

    Restart the PHP5-FPM server.

### MySQL

- `mysql-database-create(name[, charset = 'utf8'[, collision = 'utf8_general_ci']])`

    Create a database if one doesn't already exist.

- `mysql-database-restore(name, path)`

    Restore a MySQL database from an archived backup.

    This function scans for files in `path` (non-recursive) that match the format:

    ```
    ^[0-9]{8}-[0-9]{4}.tar.bz2$
    ```

    e.g., `20120101-1200.tar.bz2`. The matching files are sorted based on the file name and the newest one is picked.

    If the database `name` is empty, i.e., it doesn't contain any tables, the latest backup is piped to `mysql`.

    Example (where `/database-backups` is shared from `Vagrantfile`):

    ```bash
    mysql-database-restore 'project' '/database-backups'
    ```

- `mysql-remote-access-allow`

    Allow remote passwordless `root` access for anywhere.

    This is only a good idea if the box is configured in 'Host-Only' network mode.

- `mysql-restart`

    Restart the MySQL server and reload with new configuration.

### RubyGems

- `ruby-gems-version(package)`

    Check the installed version of a package, if any.

    When a package is not installed, `0.0.0` is returned.

- `ruby-gems-install(package[, arg[, ...]])`

    Perform an unattended installation of a package.

    If a package is already installed, it will not be re-installed or upgraded.

    Example:

    ```bash
    apt-packages-install \
      ruby1.9.1          \
      ruby1.9.1-dev      \
      rubygems1.9.1
    alternatives-ruby-install 1.9.1
    ruby-gems-install pkg-config
    ruby-gems-install sass --version '3.2.1'
    ```

### NPM (Node Package Manager)

- `npm-packages-install(package[, package[, ...]])`

    Perform an unattended **global** installation of package(s).

    Example (install UglifyJS):

    ```bash
    apt-packages-ppa 'chris-lea/node.js'
    apt-packages-update
    apt-packages-install \
      nodejs             \
      npm
    npm-packages-install uglify-js
    ```

### GitHub

- `github-gems-install(repository[, repository[, ...]])`

    Download and install RubyGems from GitHub.

    The format of each `repository` is `user/project[@branch]` where `branch` can be omitted and defaults to `master`.

    If a package is already installed, it will not be re-installed or upgraded.

    Example (install unstable versions of Sass and Compass):

    ```bash
    apt-packages-install \
      ruby1.9.1          \
      ruby1.9.1-dev      \
      rubygems1.9.1
    alternatives-ruby-install 1.9.1
    github-gems-install              \
      'ttilley/fssm'                 \
      'nex3/sass@master'             \
      'wvanbergen/chunky_png'        \
      'chriseppstein/compass@master'
    ```

- `github-php-extension-install(specification[, specification[, ...]])`

    Download and install PHP extensions from GitHub.

    The `specification` is a string which contains the repository name (mandatory), version (optional) and `./configure` arguments (optional):
    `repository@version --option --option argument`

    Example (install native PHP Redis extension from GitHub):

    ```bash
    github-php-extension-install 'nicolasff/phpredis'

    # Use a specific commit and pass arguments to `./configure`.
    github-php-extension-install 'nicolasff/phpredis@5e5fa7895f --enable-redis-igbinary'
    ```

### Environment

- `env-append(env_key, env_value[, env_comment])`

    Append an environment line to the global Bash/Zsh profile.

    `env_key` can be anything, but the following values have special meaning:

    * `PATH` - extend the `PATH` environment variable with a new directory,
    * `source` - include an external file (be careful not to `set -e`).

    Any other key would be `export`ed when a new session is initialized.

    Example (set `JAVA_HOME` for scripts that rely on it):

    ```bash
    env-append 'JAVA_HOME' "/usr/lib/jvm/java-7-openjdk-$( uname -i )"

    # $ tail -2 /etc/profile
    # # AUTO-GENERATED: JAVA_HOME.
    # [ -z "$JAVA_HOME" ] && export JAVA_HOME='/usr/lib/jvm/java-7-openjdk-i386'
    ```

Extras
------

The `ubuntu-extras.sh` file provides useful but rarely used commands.

### Archives

- `archive-file-unpack(file[, directory])`

    Unpack the given archive `file` in `directory` (created if missing). The format is guessed from the file extension.

    Example:

    ```bash
    archive-file-unpack 'rsync-HEAD.tar.gz' '/tmp/'
    ```

### Packages (e.g., ZIP distributions)

- `package-ignore-preserve(package_name, package_path, preserved_path)`

    Read a `.gitignore` file in the given `package_path` and remove matching files in `{preserved_path}/{package_name}-*`.
    This is useful when the contents of an unpacked archive are to be copied over a version-controlled directory and certain files should **not** be overwritten.

    Example:

    ```bash
    package-ignore-preserve 'hadoop' 'vendor/hadoop' '/tmp/hadoop-download'
    ```

- `package-uri-download(uri[, target])`

    Download the file at `uri` to the given directory `target` or `STDOUT`.

    Example:

    ```bash
    package-uri-download 'http://www.samba.org/ftp/rsync/nightly/rsync-HEAD.tar.gz' '/tmp'
    ```

- `package-uri-install(package_name, package_uri[, package_index[, package_path[, package_version]]])`

    Download, unpack and install the package at `package_uri` to `package_path`.
    This function does a lot internally and can be used to automate installations of certain ZIP distributions.
    The URI and path support placeholders:

    * `%name` will be substituted with the `package_name`,
    * `%path` for `package_path`,
    * `%version` for `package_version`.

    `package_index` should be a file which determines if the package has already been installed. It would usually point to a binary file inside the distribution archive. If omitted, it defaults to `{package_path}/bin/{package_name}`.

    If `package_path` is omitted, a variable `{PACKAGE_NAME}_PATH` is expected. E.g., if `package_name` is 'rsync' a variable named `RSYNC_PATH` must exist.
    If `package_version` is omitted, a variable `{PACKAGE_NAME}_VERSION` is expected.

    Requires `curl` to be installed.

    Example (installing Node.js from binary distribution):

    ```bash
    # {{{ Configuration

    NODE_PATH='/vagrant/vendor/node'
    NODE_VERSION=0.8.22
    NODE_PLATFORM=x86

    PHANTOMJS_VERSION=1.8.1
    PHANTOMJS_PATH='/vagrant/vendor/phantomjs'
    PHANTOMJS_PLATFORM=i686

    # }}}

    # Download and install Node.js from the official website.
    package-uri-install 'node' "http://nodejs.org/dist/v%version/node-v%version-linux-${NODE_PLATFORM}.tar.gz"

    # Download and install PhantomJS from the official website.
    package-uri-install 'phantomjs' "https://phantomjs.googlecode.com/files/phantomjs-%version-linux-${PHANTOMJS_PLATFORM}.tar.bz2"

    # Node.js installed as:   /vagrant/vendor/node/bin/node
    # PhantomJS installed as: /vagrant/vendor/phantomjs/bin/phantomjs
    ```

### Temporary Files

- `temporary-cleanup(tmp_path[, ...])`

    Delete the contents of each `tmp_path` and halt script with the exit code of the last executed command.

    Example (compile a project and clean up on failure):

    ```bash
    TMP_PATH="$( mktemp -d -t 'package-XXXXXXXX' )"
    git clone 'git://github.com/project/package.git' "$TMP_PATH"
    (                         \
      cd "$TMP_PATH"       && \
      ./configure          && \
      make && make install    \
    ) || temporary-cleanup "$TMP_PATH"
    ```

PostgreSQL
----------

The `ubuntu-postgres.sh` file provides functions for manipulating a PostgreSQL server instance.

- `postgres-remote-access-allow()`

    Allow remote access for the private `192.168.1.1/22` network (`192.168.0.1` through `192.168.3.254`).

    This is only a good idea if the box is configured in 'Host-Only' network mode.

- `postgres-password-reset(password)`

    Reset the `postgres` user server password. This also allows password-based authentication.

    Example:

    ```bash
    postgres-password-reset 'password'
    ```

- `postgres-autovacuum-on()`

    Turn [autovacuuming](http://www.postgresql.org/docs/9.2/static/runtime-config-autovacuum.html) on.

- `postgres-template-encoding(encoding = 'UTF8'[, ctype = 'en_GB.utf8'[, collate = ctype]])`

    Set the default database encoding and collision.
    Newly created databases will inherit those properties.

    Example (use UTF-8 for all new databases):

    ```bash
    postgres-template-encoding 'UTF8' 'en_GB.utf8'
    ```

Postfix
-------

The `ubuntu-postfix.sh` file provides functions for manipulating an SMTP server instance using Postfix.

- `smtp-sink-install(directory[, template = '%Y%m%d/%H%M.'[, port = 25[, user = 'vagrant'[, service = 'smtp-sink'[, backlog = 10]]]]])`

    Hassle-free SMTP in development. Create a new system service which logs all outgoing e-mails to disk.

    > `directory` specifies the process root directory.  
    > The single-message files are created by expanding the `template` via strftime(3) and appending a pseudo-random hexadecimal number (example: "%Y%m%d%H/%M." expands into "2006081203/05.809a62e3"). If the template contains "/" characters, missing directories are created automatically.

    `port` specifies the port on which to listen.  
    `user` switches the user privileges after opening the network socket. The user must have permissions to write in `directory`.

    `service` is an optional name of the system Upstart service. A file with this name is created under `/etc/init/`.

    `backlog` specifies the maximum length the queue of pending connections, as defined by the listen(2) system call.

    Example (log all messages to the `mail/` directory in the project root):

    ```bash
    smtp-sink-install '/vagrant/mail'
    ```

    The logged messages can be read using Thunderbird (append `.eml` to the file name).

Environment
-----------

The following variables can be defined before including the `ubuntu.sh` script:

- `SUDO[ = 'sudo']`

    Specify the prefix for all super-user commands. If your system is configured with no `sudo` command, use an empty value.

    Example (set up Google DNS on a system with no `sudo` command):

    ```bash
    SUDO=

    nameservers-local-purge
    nameservers-append '8.8.8.8'
    nameservers-append '8.8.4.4'
    ```

- `COLORS[ = 'always']`

    Use terminal escape codes to print colourised output. If you wish to disable this, use a value other than `{always,yes,true,1}`.

    Example (turn off colours in output):

    ```bash
    COLORS=never

    apt-packages-update
    ```

Goal
----

As I explore different OSes, I hope to add support for more platforms.

The functions should remain the same, so provisioning scripts are somewhat 'portable'.

Full Example
------------

The provisioning script below sets up Apache, SuExec, a virtual host for `/vagrant` (the default share) and remote `root` access to MySQL.

```bash
#!/usr/bin/env bash

# {{{ Ubuntu utilities

<%= import 'vagrant-shell-scripts/ubuntu.sh' %>

# }}}

# Use Google Public DNS for resolving domain names.
# The default is host-only DNS which may not be installed.
nameservers-local-purge
nameservers-append '8.8.8.8'
nameservers-append '8.8.4.4'

# Use a local Ubuntu mirror, results in faster downloads.
apt-mirror-pick 'bg'

# Update packages cache.
apt-packages-update

# Install VM packages.
apt-packages-install     \
  apache2-mpm-worker     \
  apache2-suexec-custom  \
  mysql-server-5.1       \
  libapache2-mod-fastcgi \
  php5-cgi               \
  php5-gd                \
  php5-mysql

# Allow modules for Apache.
apache-modules-enable actions rewrite fastcgi suexec

# Replace the default Apache site.
PHP=/usr/bin/php-cgi apache-sites-create 'vagrant'
apache-sites-disable default default-ssl
apache-sites-enable vagrant

# Restart Apache web service.
apache-restart

# Allow unsecured remote access to MySQL.
mysql-remote-access-allow

# Restart MySQL service for changes to take effect.
mysql-restart
```

### Copyright

> Copyright (c) 2012 Stan Angeloff. See [LICENSE.md](https://github.com/StanAngeloff/vagrant-shell-scripts/blob/master/LICENSE.md) for details.
