#!/bin/bash
set +e  # Don't exit on errors - we want to track them

#================================================================
# Logging and Tracking Setup
#================================================================

# Log file paths
SETUP_LOG_DIR="/tmp/spark-setup"
SETUP_LOG_FILE="${SETUP_LOG_DIR}/setup.log"
mkdir -p "${SETUP_LOG_DIR}"

# Arrays to track successes and failures
declare -a FAILED_COMMANDS=()
declare -a SUCCESSFUL_COMMANDS=()
declare -a WARNING_MESSAGES=()
declare -a INFO_MESSAGES=()

# Initialize log file with timestamp
{
    echo "========================================================================"
    echo "Spark Development Container Setup Log"
    echo "Started: $(date '+%Y-%m-%d %H:%M:%S')"
    echo "User: $(whoami)"
    echo "Platform: $(uname -a)"
    echo "========================================================================"
    echo ""
} | tee "${SETUP_LOG_FILE}"

# Function to log messages
log_message() {
    local level="$1"
    local message="$2"
    local timestamp=$(date '+%H:%M:%S')
    echo "[${timestamp}] [${level}] ${message}" | tee -a "${SETUP_LOG_FILE}"
}

# Function to track failures
track_failure() {
    local component="$1"
    local reason="$2"
    FAILED_COMMANDS+=("$component: $reason")
    log_message "FAILED" "$component - $reason"
}

# Function to track successes
track_success() {
    local component="$1"
    SUCCESSFUL_COMMANDS+=("$component")
    log_message "SUCCESS" "$component installed successfully"
}

# Function to track warnings
track_warning() {
    local message="$1"
    WARNING_MESSAGES+=("$message")
    log_message "WARNING" "$message"
}

# Function to track info
track_info() {
    local message="$1"
    INFO_MESSAGES+=("$message")
    log_message "INFO" "$message"
}

# Enable extended error reporting for line numbers
trap 'log_message "ERROR" "Script failed on line $LINENO"; show_final_report; exit 1' ERR

# Ensure SDKMAN is loaded
export SDKMAN_DIR="/usr/local/sdkman"
if [[ -s "${SDKMAN_DIR}/bin/sdkman-init.sh" ]]; then
    source "${SDKMAN_DIR}/bin/sdkman-init.sh"
    track_info "SDKMAN loaded successfully"
else
    track_warning "SDKMAN initialization file not found at ${SDKMAN_DIR}/bin/sdkman-init.sh"
fi

#================================================================
# Helper Functions for Version Detection
#================================================================

# Function to get available versions for a given SDK
get_available_versions() {
    local sdk_name="$1"
    sdk list "$sdk_name" 2>/dev/null | grep -oE '[0-9]+\.[0-9]+\.[0-9]+(-[a-zA-Z0-9]+)?' | sort -V | uniq || echo ""
}

# Function to find the latest version matching a pattern
find_latest_version() {
    local sdk_name="$1"
    local pattern="$2"  # e.g., "17\.|21\." for Java versions
    
    get_available_versions "$sdk_name" | grep -E "$pattern" | tail -1
}

# Function to install an SDK with error handling and logging
install_sdk_with_retry() {
    local sdk_name="$1"
    local version="$2"
    local default_choice="${3:-y}"  # Default to 'y' (set as default)
    
    if [ -z "$version" ]; then
        track_warning "No version found matching criteria for $sdk_name"
        return 1
    fi
    
    log_message "ACTION" "Installing $sdk_name $version..."
    
    # Suppress SDKMAN post-installation hook errors, but capture the real output
    local sdk_output
    local sdk_exit_code
    
    # Run installation, temporarily ignoring hook errors
    sdk_output=$(echo "$default_choice" | sdk install "$sdk_name" "$version" 2>&1)
    sdk_exit_code=$?
    
    # Log the full output
    echo "$sdk_output" >> "${SETUP_LOG_FILE}"
    
    # Check if the output contains the post-installation hook error
    # This error is non-critical and often occurs even when installation succeeds
    if echo "$sdk_output" | grep -q "__sdkman_post_installation_hook: command not found"; then
        track_warning "$sdk_name ($version): SDKMAN post-installation hook error (non-critical)"
        # Continue to verify installation
        sdk_exit_code=0
    fi
    
    # Verify installation by checking if binary exists
    if [ $sdk_exit_code -eq 0 ] || grep -q "Successfully installed" <<< "$sdk_output"; then
        # Reload SDKMAN to ensure the new version is available
        if [[ -s "${SDKMAN_DIR}/bin/sdkman-init.sh" ]]; then
            source "${SDKMAN_DIR}/bin/sdkman-init.sh" 2>/dev/null || true
        fi
        
        # Double-check by trying to get version
        local version_check=$(sdk current "$sdk_name" 2>/dev/null || echo "")
        if [ -n "$version_check" ]; then
            track_success "$sdk_name ($version)"
            return 0
        else
            # If we can't verify immediately, consider it success if SDK said so
            if grep -q "Successfully installed" <<< "$sdk_output"; then
                track_success "$sdk_name ($version)"
                return 0
            fi
        fi
    fi
    
    track_failure "$sdk_name ($version)" "Installation failed (exit code: $sdk_exit_code)"
    return 1
}

#================================================================
# Java Installation
#================================================================
echo ""
echo "=== Installing Java versions ==="
log_message "STAGE" "Starting Java version installation"

echo "Detecting available Java versions..."
track_info "Detecting available Java versions on this platform"

# Find available Java versions
java11=$(find_latest_version "java" "11\.")
java17=$(find_latest_version "java" "17\.")
java21=$(find_latest_version "java" "21\.")

# Log detected versions
[ -n "$java11" ] && track_info "Detected Java 11: $java11" || track_warning "Java 11 not available"
[ -n "$java17" ] && track_info "Detected Java 17: $java17" || track_warning "Java 17 not available"
[ -n "$java21" ] && track_info "Detected Java 21: $java21" || track_warning "Java 21 not available"

# Prefer Java 21, then 17, then 11
preferred_java="${java21:-${java17:-${java11}}}"

if [ -n "$java11" ]; then
    install_sdk_with_retry "java" "$java11" "y" || true
fi

if [ -n "$java17" ]; then
    install_sdk_with_retry "java" "$java17" "n" || true
fi

if [ -n "$java21" ]; then
    install_sdk_with_retry "java" "$java21" "n" || true
fi

if [ -n "$preferred_java" ]; then
    track_info "Default Java version set to: $preferred_java"
else
    track_failure "Java Installation" "No Java versions could be installed from SDKMAN"
fi

#================================================================
# Maven Installation
#================================================================
echo ""
echo "=== Installing Maven ==="
log_message "STAGE" "Starting Maven installation"

echo "Detecting available Maven versions..."
track_info "Detecting available Maven versions"

# Find latest Maven 3.9 or 3.8
maven39=$(find_latest_version "maven" "3\.9\.")
maven38=$(find_latest_version "maven" "3\.8\.")

# Log detected versions
[ -n "$maven39" ] && track_info "Detected Maven 3.9: $maven39" || track_warning "Maven 3.9 not available"
[ -n "$maven38" ] && track_info "Detected Maven 3.8: $maven38" || track_warning "Maven 3.8 not available"

# Prefer Maven 3.9, then 3.8
preferred_maven="${maven39:-${maven38}}"

if [ -n "$preferred_maven" ]; then
    install_sdk_with_retry "maven" "$preferred_maven" "y" || {
        track_failure "Maven Installation" "Failed to install Maven $preferred_maven"
    }
else
    track_failure "Maven Installation" "No Maven 3.8+ versions found - SDKMAN may be unavailable or versions not released yet"
fi


#================================================================
# Just (Task Runner) Installation
#================================================================
echo ""
echo "=== Installing Just (task runner) ==="
log_message "STAGE" "Starting Just installation"

install_just() {
    echo "Fetching latest Just release..."
    track_info "Fetching latest Just release information"
    
    JUST_VERSION=$(curl -s \
        -H "Accept: application/vnd.github+json" \
        -H "User-Agent: curl" \
        https://api.github.com/repos/casey/just/releases/latest \
        | jq -r '.tag_name // empty' \
        | sed 's/^v//')
    
    if [ -z "$JUST_VERSION" ]; then
        # Fallback version
        JUST_VERSION="1.46.0"
        track_warning "Could not detect latest Just version, using fallback: $JUST_VERSION"
        echo "  Using fallback version: $JUST_VERSION"
    else
        track_info "Found Just version: $JUST_VERSION"
        echo "  Found Just version: $JUST_VERSION"
    fi

    JUST_URL="https://github.com/casey/just/releases/download/${JUST_VERSION}/just-${JUST_VERSION}-x86_64-unknown-linux-musl.tar.gz"
    
    if wget -q -O /tmp/just.tar.gz "$JUST_URL" 2>/dev/null; then
        if tar -xzf /tmp/just.tar.gz -C /tmp >> "${SETUP_LOG_FILE}" 2>&1 && sudo mv /tmp/just /usr/local/bin/ >> "${SETUP_LOG_FILE}" 2>&1; then
            track_success "Just ($JUST_VERSION)"
            echo "  ✓ Successfully installed Just $JUST_VERSION"
            rm -f /tmp/just.tar.gz
            return 0
        else
            track_failure "Just ($JUST_VERSION)" "Failed to extract or move Just binary"
        fi
    else
        track_failure "Just ($JUST_VERSION)" "Failed to download from GitHub"
    fi
    
    echo "  ✗ Warning: Could not install Just - continuing without it"
    rm -f /tmp/just.tar.gz
    return 1
}

install_just || true

#================================================================
# AWS Tools Installation
#================================================================
echo ""
echo "=== Installing AWS tools ==="
log_message "STAGE" "Starting AWS CLI installation"

install_aws_cli() {
    echo "Installing AWS CLI v2..."
    track_info "Installing AWS CLI v2"
    
    # Update package manager
    echo "Updating system packages..."
    if sudo apt-get update -qq >> "${SETUP_LOG_FILE}" 2>&1 && \
       sudo apt-get install -y -qq jq curl git unzip >> "${SETUP_LOG_FILE}" 2>&1; then
        track_info "System packages updated successfully"
    else
        track_warning "Some system packages could not be installed"
    fi
    
    # Download and install AWS CLI
    echo "Downloading AWS CLI..."
    if curl -sSL "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" -o "/tmp/awscliv2.zip" >> "${SETUP_LOG_FILE}" 2>&1; then
        track_info "AWS CLI archive downloaded"
        
        if unzip -q /tmp/awscliv2.zip -d /tmp >> "${SETUP_LOG_FILE}" 2>&1; then
            track_info "AWS CLI archive extracted"
            
            if sudo /tmp/aws/install --update >> "${SETUP_LOG_FILE}" 2>&1; then
                track_success "AWS CLI v2"
                echo "  ✓ AWS CLI v2 installed successfully"
                rm -rf /tmp/awscliv2.zip /tmp/aws
                return 0
            else
                track_failure "AWS CLI v2" "Installation of AWS CLI binaries failed"
            fi
        else
            track_failure "AWS CLI v2" "Failed to extract AWS CLI archive"
        fi
        rm -f /tmp/awscliv2.zip /tmp/aws
    else
        track_failure "AWS CLI v2" "Failed to download from awscli.amazonaws.com"
    fi
    
    echo "  ✗ Warning: Could not install AWS CLI - continuing without it"
    return 1
}

install_aws_cli || true

# Configure AWS if config file exists
echo "Configuring AWS..."
if [ -f "/workspaces/spark/.devcontainer/aws-config" ]; then
    mkdir -p ~/.aws
    if cp /workspaces/spark/.devcontainer/aws-config ~/.aws/config >> "${SETUP_LOG_FILE}" 2>&1; then
        track_success "AWS Configuration"
        echo "  ✓ AWS configuration copied"
    else
        track_failure "AWS Configuration" "Failed to copy AWS config file"
    fi
else
    track_info "AWS config file not found - skipping AWS configuration"
    echo "  ℹ AWS config file not found - skipping AWS configuration"
fi


#================================================================
# Shell Configuration
#================================================================
echo ""
echo "=== Configuring shell environment ==="

# Set bash as default shell
if sudo chsh -s /bin/bash vscode 2>/dev/null; then
    echo "  ✓ Set bash as default shell"
else
    echo "  ℹ Could not set bash as default (already set or insufficient permissions)"
fi

# Configure bash prompt and environment
PROMPT_FILE_SRC="/workspaces/spark/.devcontainer/bash_additional.sh"
PROMPT_FILE_DST="/home/vscode/.bash_additional.sh"

if [ -f "${PROMPT_FILE_SRC}" ]; then
    cp "${PROMPT_FILE_SRC}" "${PROMPT_FILE_DST}"
    chown vscode:vscode "${PROMPT_FILE_DST}" 2>/dev/null || true
    chmod 0644 "${PROMPT_FILE_DST}" 2>/dev/null || true
    echo "  ✓ Bash additional config installed"
else
    echo "  ℹ Bash additional config not found - using defaults"
fi

# Add source line to .bashrc if not already present
if ! grep -q 'bash_additional\.sh' /home/vscode/.bashrc 2>/dev/null; then
    cat >> /home/vscode/.bashrc << 'EOF'

if [ -f "$HOME/.bash_additional.sh" ]; then
  source "$HOME/.bash_additional.sh"
fi
EOF
    echo "  ✓ Updated .bashrc with bash_additional.sh sourcing"
fi

#================================================================
# Spark Dependencies Pre-fetching
#================================================================
echo ""
echo "=== Pre-fetching Spark build dependencies ==="
echo "This may take several minutes on first run..."

if [ -d "/workspaces/spark" ] && [ -f "/workspaces/spark/pom.xml" ]; then
    cd /workspaces/spark
    
    # Pre-fetch Maven dependencies (non-critical, so we don't fail on error)
    if command -v mvn &> /dev/null; then
        if mvn dependency:go-offline -DskipTests -q 2>/dev/null; then
            echo "  ✓ Successfully pre-fetched dependencies"
        else
            echo "  ⚠️  Partial dependency fetch completed (some mirrors may be unavailable)"
        fi
    else
        echo "  ℹ Maven not available - skipping dependency pre-fetch"
    fi
else
    echo "  ℹ Workspace or pom.xml not yet available - skipping dependency pre-fetch"
fi

#================================================================
# Shell Configuration
#================================================================
echo ""
echo "=== Configuring shell environment ==="
log_message "STAGE" "Configuring shell environment"

# Set bash as default shell
if sudo chsh -s /bin/bash vscode 2>/dev/null; then
    track_success "Bash Default Shell"
    echo "  ✓ Set bash as default shell"
else
    track_warning "Could not set bash as default (already set or insufficient permissions)"
    echo "  ℹ Could not set bash as default (already set or insufficient permissions)"
fi

# Configure bash prompt and environment
PROMPT_FILE_SRC="/workspaces/spark/.devcontainer/bash_additional.sh"
PROMPT_FILE_DST="/home/vscode/.bash_additional.sh"

if [ -f "${PROMPT_FILE_SRC}" ]; then
    if cp "${PROMPT_FILE_SRC}" "${PROMPT_FILE_DST}" >> "${SETUP_LOG_FILE}" 2>&1; then
        chown vscode:vscode "${PROMPT_FILE_DST}" 2>/dev/null || true
        chmod 0644 "${PROMPT_FILE_DST}" 2>/dev/null || true
        track_success "Bash Additional Config"
        echo "  ✓ Bash additional config installed"
    else
        track_failure "Bash Additional Config" "Failed to copy bash_additional.sh"
    fi
else
    track_info "Bash additional config file not found - using defaults"
    echo "  ℹ Bash additional config not found - using defaults"
fi

# Add source line to .bashrc if not already present
if ! grep -q 'bash_additional\.sh' /home/vscode/.bashrc 2>/dev/null; then
    if cat >> /home/vscode/.bashrc << 'EOF' >> "${SETUP_LOG_FILE}" 2>&1
if [ -f "$HOME/.bash_additional.sh" ]; then
  source "$HOME/.bash_additional.sh"
fi
EOF
    then
        track_success "Bashrc Update"
        echo "  ✓ Updated .bashrc with bash_additional.sh sourcing"
    else
        track_failure "Bashrc Update" "Failed to update .bashrc"
    fi
fi

#================================================================
# Spark Dependencies Pre-fetching
#================================================================
echo ""
echo "=== Pre-fetching Spark build dependencies ==="
log_message "STAGE" "Pre-fetching Spark dependencies"

echo "This may take several minutes on first run..."
track_info "Starting dependency pre-fetch (this may take several minutes)"

if [ -d "/workspaces/spark" ] && [ -f "/workspaces/spark/pom.xml" ]; then
    cd /workspaces/spark
    
    # Pre-fetch Maven dependencies (non-critical, so we don't fail on error)
    if command -v mvn &> /dev/null; then
        echo "Running: mvn dependency:go-offline..."
        if mvn dependency:go-offline -DskipTests -q >> "${SETUP_LOG_FILE}" 2>&1; then
            track_success "Spark Dependencies Pre-fetch"
            echo "  ✓ Successfully pre-fetched dependencies"
        else
            track_warning "Partial dependency fetch completed (some mirrors may be unavailable)"
            echo "  ⚠️  Partial dependency fetch completed (some mirrors may be unavailable)"
        fi
    else
        track_failure "Spark Dependencies Pre-fetch" "Maven not available - skipping dependency pre-fetch"
        echo "  ℹ Maven not available - skipping dependency pre-fetch"
    fi
else
    track_info "Workspace or pom.xml not yet available - skipping dependency pre-fetch"
    echo "  ℹ Workspace or pom.xml not yet available - skipping dependency pre-fetch"
fi

#================================================================
# Final Report Function
#================================================================

show_final_report() {
    local success_count=${#SUCCESSFUL_COMMANDS[@]}
    local failure_count=${#FAILED_COMMANDS[@]}
    local warning_count=${#WARNING_MESSAGES[@]}
    
    echo ""
    log_message "REPORT" "Setup process completed"
    log_message "STATS" "Successes: $success_count, Failures: $failure_count, Warnings: $warning_count"
    
    echo ""
    echo "╔════════════════════════════════════════════════════════════════╗"
    echo "║          Spark Development Container Setup Report              ║"
    echo "╚════════════════════════════════════════════════════════════════╝"
    echo ""
    
    # Summary stats
    echo "📊 Setup Summary:"
    echo "   ✓ Successful: $success_count"
    echo "   ✗ Failed: $failure_count"
    echo "   ⚠️  Warnings: $warning_count"
    echo ""
    
    # Successful installations
    if [ $success_count -gt 0 ]; then
        echo "✅ Successfully Installed:"
        for cmd in "${SUCCESSFUL_COMMANDS[@]}"; do
            echo "   • $cmd"
        done
        echo ""
    fi
    
    # Failed installations - IMPORTANT FOR DEVELOPERS
    if [ $failure_count -gt 0 ]; then
        echo "❌ Failed Installations (Developer Action Required):"
        for failed in "${FAILED_COMMANDS[@]}"; do
            echo "   • $failed"
        done
        echo ""
        echo "💡 Troubleshooting Steps:"
        echo "   1. Check the full log at: ${SETUP_LOG_FILE}"
        echo "   2. For SDKMAN issues:"
        echo "      - Check SDKMAN is installed: ls ${SDKMAN_DIR}/bin/sdkman-init.sh"
        echo "      - Reload SDKMAN: source ${SDKMAN_DIR}/bin/sdkman-init.sh"
        echo "      - List available versions: sdk list java (or maven)"
        echo "   3. For network issues, verify internet connectivity"
        echo "   4. Try running the setup script again"
        echo ""
    fi
    
    # Warnings
    if [ $warning_count -gt 0 ]; then
        echo "⚠️  Warnings (Review Recommended):"
        for warning in "${WARNING_MESSAGES[@]}"; do
            echo "   • $warning"
        done
        echo ""
    fi
    
    # Installed versions
    echo "🔍 Installed Component Versions:"
    if command -v java &> /dev/null; then
        echo "   • Java: $(java -version 2>&1 | head -1)"
    else
        echo "   • Java: ⚠️  Not available"
    fi
    
    if command -v mvn &> /dev/null; then
        echo "   • Maven: $(mvn -v 2>&1 | head -1)"
    else
        echo "   • Maven: ⚠️  Not available"
    fi
    
    if command -v just &> /dev/null; then
        echo "   • Just: $(just --version 2>/dev/null)"
    else
        echo "   • Just: ⚠️  Not available"
    fi
    
    if command -v aws &> /dev/null; then
        echo "   • AWS CLI: $(aws --version 2>&1)"
    else
        echo "   • AWS CLI: ⚠️  Not available"
    fi
    echo ""
    
    # Next steps
    echo "📝 Next Steps:"
    echo "   1. Review the full log: cat ${SETUP_LOG_FILE}"
    echo "   2. Source your shell config: source ~/.bashrc"
    echo "   3. Verify SDK versions: sdk current"
    echo "   4. Start building: mvn clean install"
    echo ""
    
    # Log details
    echo "📂 Log File: ${SETUP_LOG_FILE}"
    echo ""
    
    {
        echo ""
        echo "========================================================================"
        echo "Setup completed at: $(date '+%Y-%m-%d %H:%M:%S')"
        echo "Total successes: $success_count"
        echo "Total failures: $failure_count"
        echo "Total warnings: $warning_count"
        echo "========================================================================"
    } >> "${SETUP_LOG_FILE}"
}

#================================================================
# Setup Summary
#================================================================
echo ""
echo "=== Setup Process Complete ==="
log_message "STAGE" "Setup process finished, generating report"

show_final_report

# Exit with appropriate code
if [ ${#FAILED_COMMANDS[@]} -gt 0 ]; then
    echo "⚠️  Setup completed with some failures. See report above."
    exit 0  # Don't fail - allow container to start even with some failures
else
    echo "✅ Setup completed successfully!"
    exit 0
fi