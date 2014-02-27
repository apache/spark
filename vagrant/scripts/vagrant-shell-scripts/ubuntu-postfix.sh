#!/usr/bin/env bash

set -e

# {{{ SMTP

# Install an SMTP sink service which logs all outgoing e-mails to disk.
smtp-sink-install() {
  log-operation "$FUNCNAME" "$@"
  local smtp_sink_directory="$1" \
        smtp_sink_template="${2:-%Y%m%d/%H%M.}" \
        smtp_port="${3:-25}" \
        smtp_user="${4:-vagrant}" \
        smtp_service="${5:-smtp-sink}" \
        smtp_hard_bounce_reply="550 5.3.0 Sink'd." \
        smtp_hostname='localhost.localdomain' \
        smtp_backlog="${6:-10}"
  if [ -z "$smtp_sink_directory" ]; then
    echo -e "${ERROR_BOLD}E: ${ERROR_NORMAL}You must specify 'smtp_sink_directory' to '${FUNCNAME}' at position '1'.${RESET}" 1>&2
    exit 1
  fi
  dependency-install 'postfix'
  # If Postfix is currently running, stop it.
  QUIET=1 system-service postfix status && system-service postfix stop || :
  # Stop Postfix from running at start up.
  $SUDO update-rc.d -f 'postfix' remove 1>/dev/null
  # Kill any running smtp-sink services, try graceful shutdown first.
  QUIET=1 system-service "$smtp_service" stop 1>/dev/null 2>&1 || :
  $SUDO killall -q -9 smtp-sink || :
  # Create a new service to log all e-mails to disk.
  cat <<-EOD | $SUDO tee "/etc/init/${smtp_service}.conf" 1>/dev/null
# ${smtp_service}
#
# SMTP server which logs all outgoing e-mails to disk.

description	"${smtp_service}, logs all outgoing e-mails"

start on runlevel [2345]
stop on runlevel [!2345]

console log

respawn
respawn limit 3 5

pre-start exec mkdir -p "$smtp_sink_directory"

exec start-stop-daemon --start --exec "$( which smtp-sink )" -- \\
  -u "$smtp_user" \\
  -R "$( echo "$smtp_sink_directory" | sed -e 's#/$##' )/" -d "$smtp_sink_template" \\
  -f '.' -B "$smtp_hard_bounce_reply" \\
  -h "$smtp_hostname" "$smtp_port" "$smtp_backlog"
EOD
  # Start the service. It will also run on next start up
  system-service "$smtp_service" start
}

# }}}

# {{{ Dependency Management

# Create associations for packages we are going to install.
dependency-package-associate 'postfix' 'postfix'

# }}}
