#!/bin/bash

# Script to add tracing to all functions with r *http.Request parameter
# that don't already have it

set -e

LOG_IMPORT='	"github.com/VictoriaMetrics/VictoriaMetrics/lib/logger"'
TRACE_CODE='	span := logger.TraceRequest(r)
	defer span.End()

'

# Function to check if a file has the log import
has_log_import() {
    local file="$1"
    grep -q 'github.com/VictoriaMetrics/VictoriaMetrics/lib/logger"' "$file"
}

# Function to add log import to a file
add_log_import() {
    local file="$1"

    # Find the last import line and add the log import after it
    # Look for the import block and add our import
    if grep -q "import (" "$file"; then
        # Add as the last import before closing parenthesis
        sed -i '/^)$/i\
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/logger"' "$file"
    fi
}

# Function to check if a function already has tracing
has_tracing() {
    local file="$1"
    local func_line="$2"

    # Look for the tracing pattern within the next 10 lines after the function declaration
    local end_line=$((func_line + 10))
    sed -n "${func_line},${end_line}p" "$file" | grep -q "span := logger\.TraceRequest(r)"
}

# Function to add tracing to a specific function
add_tracing_to_function() {
    local file="$1"
    local func_line="$2"

    # Find the opening brace of the function
    local brace_line
    brace_line=$(sed -n "${func_line},\$p" "$file" | grep -n "{" | head -1 | cut -d: -f1)
    if [ -z "$brace_line" ]; then
        echo "Warning: Could not find opening brace for function at line $func_line in $file"
        return
    fi

    # Calculate actual line number
    local actual_brace_line=$((func_line + brace_line - 1))

    # Insert tracing code after the opening brace
    local temp_file=$(mktemp)
    {
        head -n "$actual_brace_line" "$file"
        echo "$TRACE_CODE"
        tail -n +$((actual_brace_line + 1)) "$file"
    } > "$temp_file"

    mv "$temp_file" "$file"
}

# Process all Go files
process_file() {
    local file="$1"

    echo "Processing $file..."

    local modified=false

    # Find all functions with r *http.Request parameter
    # Look for pattern: func functionName(r *http.Request
    local func_lines
    func_lines=$(grep -n "func [^(]*([^)]*r \*http\.Request" "$file" | cut -d: -f1 || true)

    if [ -z "$func_lines" ]; then
        return
    fi

    # Process each function (in reverse order to avoid line number changes affecting later functions)
    for func_line in $(echo "$func_lines" | sort -nr); do
        if ! has_tracing "$file" "$func_line"; then
            echo "  Adding tracing to function at line $func_line"
            add_tracing_to_function "$file" "$func_line"
            modified=true
        fi
    done

    # Add log import if we modified the file and it doesn't have the import
    if [ "$modified" = true ] && ! has_log_import "$file"; then
        echo "  Adding log import to $file"
        add_log_import "$file"
    fi
}

# Find all Go files excluding test files and generated files
find_go_files() {
    find . -name "*.go" \
        -not -path "./vendor/*" \
        -not -path "./.git/*" \
        -not -name "*_test.go" \
        -not -name "zz_generated*" \
        -not -name "*pb.go"
}

# Main execution
main() {
    echo "Adding tracing to functions with ctx context.Context parameter..."
    echo "Working directory: $(pwd)"

    # Process each Go file
    while IFS= read -r file; do
        process_file "$file"
    done < <(find_go_files)

    echo "Done! Please review the changes and run 'go fmt ./...' to format the code."
    echo "You may also want to run 'go mod tidy' and test the build."
}

# Run the script
main "$@"
